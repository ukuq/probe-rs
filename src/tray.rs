//! Windows notification-area companion.
//!
//! The monitoring agent runs as SYSTEM in session 0, which cannot display UI
//! in an interactive user's session. The installer launches this lightweight
//! `--tray` mode separately for the signed-in user.

use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{anyhow, Result};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HWND, LPARAM, LRESULT, POINT, WPARAM,
};
use windows_sys::Win32::System::Console::GetConsoleWindow;
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::System::Threading::CreateMutexW;
use windows_sys::Win32::UI::Shell::{
    Shell_NotifyIconW, NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE, NOTIFYICONDATAW,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, CreateWindowExW, DefWindowProcW, DestroyMenu, DestroyWindow,
    DispatchMessageW, GetCursorPos, GetMessageW, LoadIconW, MessageBoxW, PostMessageW,
    PostQuitMessage, RegisterClassW, RegisterWindowMessageW, SetForegroundWindow, ShowWindow,
    TrackPopupMenu, TranslateMessage, IDI_INFORMATION, MB_OK, MF_GRAYED, MF_SEPARATOR, MF_STRING,
    MSG, SW_HIDE, TPM_RETURNCMD, TPM_RIGHTBUTTON, WM_APP, WM_DESTROY, WM_LBUTTONDBLCLK, WM_NULL,
    WM_RBUTTONUP, WNDCLASSW,
};

const TRAY_ICON_ID: u32 = 1;
const WM_TRAY_ICON: u32 = WM_APP + 1;
const MENU_ABOUT: usize = 1001;
const MENU_EXIT: usize = 1002;

static TASKBAR_CREATED: AtomicU32 = AtomicU32::new(0);

pub fn run() -> Result<()> {
    // A console-subsystem executable is retained for useful foreground agent
    // diagnostics. Hide that console immediately when running only the tray.
    unsafe {
        let console = GetConsoleWindow();
        if !console.is_null() {
            ShowWindow(console, SW_HIDE);
        }
    }

    let mutex_name = wide("Local\\probe-rs-tray");
    let mutex = unsafe { CreateMutexW(null(), 0, mutex_name.as_ptr()) };
    if mutex.is_null() {
        return Err(last_error("创建托盘单实例锁失败"));
    }
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        unsafe { CloseHandle(mutex) };
        return Ok(());
    }

    let result = unsafe { run_message_loop() };
    unsafe { CloseHandle(mutex) };
    result
}

unsafe fn run_message_loop() -> Result<()> {
    let instance = GetModuleHandleW(null());
    if instance.is_null() {
        return Err(last_error("获取程序模块失败"));
    }

    let taskbar_message = RegisterWindowMessageW(wide("TaskbarCreated").as_ptr());
    if taskbar_message == 0 {
        return Err(last_error("注册任务栏恢复消息失败"));
    }
    TASKBAR_CREATED.store(taskbar_message, Ordering::Relaxed);

    let class_name = wide("probe-rs-tray-window");
    let window_class = WNDCLASSW {
        lpfnWndProc: Some(window_proc),
        hInstance: instance,
        lpszClassName: class_name.as_ptr(),
        ..Default::default()
    };
    if RegisterClassW(&window_class) == 0 {
        return Err(last_error("注册托盘窗口失败"));
    }

    let window_title = wide("probe-rs tray");
    let window = CreateWindowExW(
        0,
        class_name.as_ptr(),
        window_title.as_ptr(),
        0,
        0,
        0,
        0,
        0,
        null_mut(),
        null_mut(),
        instance,
        null(),
    );
    if window.is_null() {
        return Err(last_error("创建托盘窗口失败"));
    }

    if let Err(error) = add_tray_icon(window) {
        DestroyWindow(window);
        return Err(error);
    }

    let mut message = MSG::default();
    loop {
        let status = GetMessageW(&mut message, null_mut(), 0, 0);
        if status == -1 {
            DestroyWindow(window);
            return Err(last_error("读取托盘窗口消息失败"));
        }
        if status == 0 {
            break;
        }
        TranslateMessage(&message);
        DispatchMessageW(&message);
    }
    Ok(())
}

unsafe extern "system" fn window_proc(
    window: HWND,
    message: u32,
    wparam: WPARAM,
    lparam: LPARAM,
) -> LRESULT {
    let taskbar_message = TASKBAR_CREATED.load(Ordering::Relaxed);
    if taskbar_message != 0 && message == taskbar_message {
        let _ = add_tray_icon(window);
        return 0;
    }

    match message {
        WM_TRAY_ICON => {
            match lparam as u32 {
                WM_RBUTTONUP => show_context_menu(window),
                WM_LBUTTONDBLCLK => show_about(window),
                _ => {}
            }
            0
        }
        WM_DESTROY => {
            remove_tray_icon(window);
            PostQuitMessage(0);
            0
        }
        _ => DefWindowProcW(window, message, wparam, lparam),
    }
}

unsafe fn add_tray_icon(window: HWND) -> Result<()> {
    let icon = LoadIconW(null_mut(), IDI_INFORMATION);
    if icon.is_null() {
        return Err(last_error("加载托盘图标失败"));
    }

    let mut data = NOTIFYICONDATAW {
        cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
        hWnd: window,
        uID: TRAY_ICON_ID,
        uFlags: NIF_ICON | NIF_MESSAGE | NIF_TIP,
        uCallbackMessage: WM_TRAY_ICON,
        hIcon: icon,
        ..Default::default()
    };
    copy_wide(
        &mut data.szTip,
        &format!("probe-rs {}", env!("CARGO_PKG_VERSION")),
    );
    if Shell_NotifyIconW(NIM_ADD, &data) == 0 {
        return Err(last_error("添加托盘图标失败"));
    }
    Ok(())
}

unsafe fn remove_tray_icon(window: HWND) {
    let data = NOTIFYICONDATAW {
        cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
        hWnd: window,
        uID: TRAY_ICON_ID,
        ..Default::default()
    };
    Shell_NotifyIconW(NIM_DELETE, &data);
}

unsafe fn show_context_menu(window: HWND) {
    let menu = CreatePopupMenu();
    if menu.is_null() {
        return;
    }

    let title = wide(&format!("probe-rs {}", env!("CARGO_PKG_VERSION")));
    let about = wide("关于 probe-rs");
    let exit = wide("退出托盘（探针继续运行）");
    AppendMenuW(menu, MF_STRING | MF_GRAYED, 0, title.as_ptr());
    AppendMenuW(menu, MF_SEPARATOR, 0, null());
    AppendMenuW(menu, MF_STRING, MENU_ABOUT, about.as_ptr());
    AppendMenuW(menu, MF_STRING, MENU_EXIT, exit.as_ptr());

    let mut cursor = POINT::default();
    if GetCursorPos(&mut cursor) != 0 {
        SetForegroundWindow(window);
        let command = TrackPopupMenu(
            menu,
            TPM_RETURNCMD | TPM_RIGHTBUTTON,
            cursor.x,
            cursor.y,
            0,
            window,
            null(),
        ) as usize;
        PostMessageW(window, WM_NULL, 0, 0);
        match command {
            MENU_ABOUT => show_about(window),
            MENU_EXIT => {
                DestroyWindow(window);
            }
            _ => {}
        }
    }
    DestroyMenu(menu);
}

unsafe fn show_about(window: HWND) {
    let title = wide("probe-rs");
    let body = wide(&format!(
        "probe-rs 托盘伴随程序 {}\n\n后台探针由 SYSTEM 计划任务托管。\n退出托盘不会停止探针。",
        env!("CARGO_PKG_VERSION")
    ));
    MessageBoxW(window, body.as_ptr(), title.as_ptr(), MB_OK);
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn copy_wide(target: &mut [u16], value: &str) {
    for (target, value) in target.iter_mut().zip(value.encode_utf16()) {
        *target = value;
    }
}

fn last_error(action: &str) -> anyhow::Error {
    anyhow!("{action}: {}", std::io::Error::last_os_error())
}
