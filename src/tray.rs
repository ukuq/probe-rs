//! Windows notification-area companion.
//!
//! The monitoring agent runs as SYSTEM in session 0, which cannot display UI
//! in an interactive user's session. The installer launches this lightweight
//! `--tray` mode separately for the signed-in user.

use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HWND, INVALID_HANDLE_VALUE, LPARAM, LRESULT,
    POINT, WPARAM,
};
use windows_sys::Win32::System::Console::GetConsoleWindow;
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W, TH32CS_SNAPPROCESS,
};
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::System::Threading::{
    CreateMutexW, OpenProcess, QueryFullProcessImageNameW, TerminateProcess,
    CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, PROCESS_NAME_WIN32,
    PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_TERMINATE,
};
use windows_sys::Win32::UI::Shell::{
    ShellExecuteW, Shell_NotifyIconW, NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE,
    NOTIFYICONDATAW,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, CreateWindowExW, DefWindowProcW, DestroyMenu, DestroyWindow,
    DispatchMessageW, GetCursorPos, GetMessageW, LoadIconW, MessageBoxW, PostMessageW,
    PostQuitMessage, RegisterClassW, RegisterWindowMessageW, SetForegroundWindow, ShowWindow,
    TrackPopupMenu, TranslateMessage, IDI_INFORMATION, IDOK, IDRETRY, MB_ICONERROR,
    MB_ICONINFORMATION, MB_ICONWARNING, MB_OK, MB_OKCANCEL, MB_RETRYCANCEL, MF_GRAYED,
    MF_SEPARATOR, MF_STRING, MSG, SW_HIDE, TPM_RETURNCMD, TPM_RIGHTBUTTON, WM_APP, WM_DESTROY,
    WM_LBUTTONDBLCLK, WM_NULL, WM_RBUTTONUP, WNDCLASSW,
};

const TASK_NAME: &str = "probe-rs";
const TRAY_ICON_ID: u32 = 1;
const WM_TRAY_ICON: u32 = WM_APP + 1;
const MENU_START: usize = 1001;
const MENU_STOP: usize = 1002;
const MENU_RESTART: usize = 1003;
const MENU_OPEN_CONFIG: usize = 1004;
const MENU_ABOUT: usize = 1005;
const MENU_EXIT: usize = 1006;

static TASKBAR_CREATED: AtomicU32 = AtomicU32::new(0);
static RUNTIME_OPTIONS: OnceLock<RuntimeOptions> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeOptions {
    user_mode: bool,
    config_path: PathBuf,
}

impl RuntimeOptions {
    fn scope_name(&self) -> &'static str {
        if self.user_mode {
            "User"
        } else {
            "Machine"
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlAction {
    Start,
    Stop,
    Restart,
    EditConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlRequest {
    action: ControlAction,
    options: RuntimeOptions,
}

pub fn run_control_if_requested() -> Result<bool> {
    let Some(request) = parse_control_request(std::env::args_os().skip(1))? else {
        return Ok(false);
    };

    if let Err(error) = execute_control(&request) {
        unsafe {
            show_message(
                null_mut(),
                "probe-rs 操作失败",
                &format!("{error:#}"),
                MB_OK | MB_ICONERROR,
            );
        }
        return Err(error);
    }
    Ok(true)
}

fn parse_control_request(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Option<ControlRequest>> {
    let mut args = args.into_iter();
    let Some(flag) = args.next() else {
        return Ok(None);
    };
    if flag != OsStr::new("--tray-control") {
        return Ok(None);
    }

    let action = match args
        .next()
        .context("--tray-control requires an action")?
        .to_str()
    {
        Some("start") => ControlAction::Start,
        Some("stop") => ControlAction::Stop,
        Some("restart") => ControlAction::Restart,
        Some("edit-config" | "open-config") => ControlAction::EditConfig,
        Some(action) => bail!("unknown --tray-control action: {action}"),
        None => bail!("--tray-control action is not valid Unicode"),
    };
    let options = parse_runtime_options(args)?;
    Ok(Some(ControlRequest { action, options }))
}

fn parse_runtime_options(args: impl IntoIterator<Item = OsString>) -> Result<RuntimeOptions> {
    let mut user_mode = false;
    let mut config_path = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == OsStr::new("--user-mode") {
            user_mode = true;
            continue;
        }
        if arg == OsStr::new("--tray") {
            continue;
        }
        if arg == OsStr::new("--config") || arg == OsStr::new("-c") {
            let value = args.next().context("--config requires a path")?;
            if value.is_empty() {
                bail!("--config requires a path");
            }
            config_path = Some(PathBuf::from(value));
            continue;
        }
        if let Some(arg) = arg.to_str() {
            if let Some(value) = arg.strip_prefix("--config=") {
                if value.is_empty() {
                    bail!("--config= requires a path");
                }
                config_path = Some(PathBuf::from(value));
                continue;
            }
        }
        bail!("unsupported tray argument: {}", arg.to_string_lossy());
    }
    Ok(RuntimeOptions {
        user_mode,
        config_path: config_path.unwrap_or_else(|| default_config_path(user_mode)),
    })
}

fn default_config_path(user_mode: bool) -> PathBuf {
    let base = if user_mode {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Users\Default\AppData\Local"))
    } else {
        std::env::var_os("ProgramData")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
    };
    base.join("probe-rs").join("config.toml")
}

fn execute_control(request: &ControlRequest) -> Result<()> {
    match request.action {
        ControlAction::Start => start_agent(&request.options),
        ControlAction::Stop => stop_agent(&request.options),
        ControlAction::Restart => restart_agent(&request.options),
        ControlAction::EditConfig => edit_config(&request.options.config_path),
    }
}

fn start_agent(options: &RuntimeOptions) -> Result<()> {
    let agent = installed_agent_path()?;
    if agent_running(&agent, !options.user_mode)
        .context("failed to inspect probe-rs process state")?
    {
        return Ok(());
    }
    if options.user_mode {
        let directory = agent
            .parent()
            .context("installed agent has no parent directory")?;
        let mut command = Command::new(&agent);
        command
            .arg("--user-mode")
            .arg("--config")
            .arg(&options.config_path)
            .current_dir(directory)
            .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
        command
            .spawn()
            .with_context(|| format!("failed to start {}", agent.display()))?;
    } else {
        run_schtasks(&["/Change", "/TN", TASK_NAME, "/ENABLE"])?;
        run_schtasks(&["/Run", "/TN", TASK_NAME])?;
    }
    wait_for_agent_state(&agent, !options.user_mode, true)
}

fn stop_agent(options: &RuntimeOptions) -> Result<()> {
    let agent = installed_agent_path()?;
    let process_ids = agent_process_ids(&agent, !options.user_mode)?;
    if process_ids.is_empty() {
        return Ok(());
    }
    if options.user_mode {
        for process_id in process_ids {
            terminate_process(process_id)?;
        }
    } else {
        run_schtasks(&["/End", "/TN", TASK_NAME])?;
    }
    wait_for_agent_state(&agent, !options.user_mode, false)
}

fn restart_agent(options: &RuntimeOptions) -> Result<()> {
    stop_agent(options)?;
    start_agent(options)
}

fn run_schtasks(arguments: &[&str]) -> Result<()> {
    use std::os::windows::process::CommandExt;

    let status = Command::new("schtasks.exe")
        .args(arguments)
        .creation_flags(CREATE_NO_WINDOW)
        .status()
        .context("failed to launch schtasks.exe")?;
    if !status.success() {
        bail!("schtasks.exe failed with status {status}");
    }
    Ok(())
}

fn wait_for_agent_state(
    agent: &Path,
    include_inaccessible: bool,
    expected_running: bool,
) -> Result<()> {
    for _ in 0..50 {
        if agent_running(agent, include_inaccessible)
            .context("failed to inspect probe-rs process state")?
            == expected_running
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    if expected_running {
        bail!("probe-rs task was started, but its process did not remain running");
    }
    bail!("probe-rs task was stopped, but its process is still running");
}

fn terminate_process(process_id: u32) -> Result<()> {
    let process = unsafe { OpenProcess(PROCESS_TERMINATE, 0, process_id) };
    if process.is_null() {
        return Err(last_error(&format!("打开 probe-rs PID {process_id} 失败")));
    }
    let terminated = unsafe { TerminateProcess(process, 0) };
    let error = (terminated == 0).then(std::io::Error::last_os_error);
    unsafe { CloseHandle(process) };
    if let Some(error) = error {
        bail!("停止 probe-rs PID {process_id} 失败: {error}");
    }
    Ok(())
}

fn edit_config(config_path: &Path) -> Result<()> {
    let original = std::fs::read_to_string(config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_dir = config_path
        .parent()
        .context("configuration path has no parent directory")?;
    let (_edit_dir, edit_path) = create_edit_copy(config_dir, &original)?;

    let _editor = Command::new("notepad.exe")
        .arg(&edit_path)
        .spawn()
        .with_context(|| format!("failed to open edit copy {}", edit_path.display()))?;

    let proceed = unsafe {
        show_message(
            null_mut(),
            "probe-rs 安全编辑配置",
            "记事本中打开的是临时副本，正式配置尚未改变。\n\n请完成编辑并保存、关闭记事本，然后点击“确定”进行校验并应用；点击“取消”将放弃本次编辑。",
            MB_OKCANCEL | MB_ICONINFORMATION,
        )
    };
    if proceed != IDOK {
        return Ok(());
    }

    loop {
        let edited = std::fs::read_to_string(&edit_path)
            .with_context(|| format!("failed to read edit copy {}", edit_path.display()))?;
        if edited == original {
            let txt_copy = edit_path.with_extension("toml.txt");
            let detail = if txt_copy.is_file() {
                format!(
                    "检测到记事本把内容另存为了：\n{}\n\n请回到记事本，使用“另存为”覆盖原文件 probe-rs-edit.toml，并将文件类型选为“所有文件”，然后重试。",
                    txt_copy.display()
                )
            } else {
                "未检测到配置内容变化。请确认已在记事本中保存 probe-rs-edit.toml，然后重试。"
                    .to_string()
            };
            let retry = unsafe {
                show_message(
                    null_mut(),
                    "probe-rs 配置尚未保存",
                    &detail,
                    MB_RETRYCANCEL | MB_ICONWARNING,
                )
            };
            if retry != IDRETRY {
                return Ok(());
            }
            continue;
        }
        match crate::config::persist_edited_text(config_path, &original, &edited) {
            Ok(backup_path) => {
                unsafe {
                    show_message(
                        null_mut(),
                        "probe-rs 配置已保存",
                        &format!(
                            "配置已通过完整校验并原子保存。\n备份：{}\n\n采集类设置会自动热加载；连接地址、凭据或 Reporter 数量变化需要重启探针。",
                            backup_path.display()
                        ),
                        MB_OK | MB_ICONINFORMATION,
                    );
                }
                return Ok(());
            }
            Err(error) => {
                let retry = unsafe {
                    show_message(
                        null_mut(),
                        "probe-rs 配置校验失败",
                        &format!(
                            "{error:#}\n\n正式配置未改变。请回到记事本修正并保存，然后点击“重试”；点击“取消”放弃本次编辑。"
                        ),
                        MB_RETRYCANCEL | MB_ICONWARNING,
                    )
                };
                if retry != IDRETRY {
                    return Ok(());
                }
            }
        }
    }
}

fn create_edit_copy(config_dir: &Path, original: &str) -> Result<(tempfile::TempDir, PathBuf)> {
    use std::io::Write;

    // Keep each editor session isolated while giving Notepad a normal, stable
    // filename. A leading-dot random filename can make Notepad's Save As flow
    // append `.txt`, leaving the file read below unchanged.
    let edit_dir = tempfile::Builder::new()
        .prefix(".probe-rs-edit-")
        .tempdir_in(config_dir)
        .with_context(|| {
            format!(
                "failed to create edit directory in {}",
                config_dir.display()
            )
        })?;
    let edit_path = edit_dir.path().join("probe-rs-edit.toml");
    let mut staged = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&edit_path)
        .with_context(|| format!("failed to create edit copy {}", edit_path.display()))?;
    staged.write_all(original.as_bytes())?;
    staged.sync_all()?;
    drop(staged);
    Ok((edit_dir, edit_path))
}

fn runtime_options() -> &'static RuntimeOptions {
    RUNTIME_OPTIONS
        .get()
        .expect("tray runtime options initialized")
}

fn installed_agent_path() -> Result<PathBuf> {
    let executable = std::env::current_exe().context("failed to locate tray executable")?;
    let directory = executable
        .parent()
        .context("tray executable has no parent directory")?;
    Ok(directory.join("probe-rs.exe"))
}

fn agent_running(agent: &Path, include_inaccessible: bool) -> std::io::Result<bool> {
    Ok(!agent_process_ids(agent, include_inaccessible)?.is_empty())
}

fn agent_process_ids(agent: &Path, include_inaccessible: bool) -> std::io::Result<Vec<u32>> {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }

    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    let mut process_ids = Vec::new();
    let mut has_entry = unsafe { Process32FirstW(snapshot, &mut entry) } != 0;
    while has_entry {
        let name_len = entry
            .szExeFile
            .iter()
            .position(|character| *character == 0)
            .unwrap_or(entry.szExeFile.len());
        if entry.th32ProcessID != std::process::id()
            && String::from_utf16_lossy(&entry.szExeFile[..name_len])
                .eq_ignore_ascii_case("probe-rs.exe")
            && process_image_matches(entry.th32ProcessID, agent, include_inaccessible)
        {
            process_ids.push(entry.th32ProcessID);
        }
        has_entry = unsafe { Process32NextW(snapshot, &mut entry) } != 0;
    }
    unsafe { CloseHandle(snapshot) };
    process_ids.sort_unstable();
    Ok(process_ids)
}

fn process_image_matches(process_id: u32, expected: &Path, include_inaccessible: bool) -> bool {
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
    if process.is_null() {
        return include_inaccessible;
    }
    let mut buffer = [0_u16; 1024];
    let mut size = buffer.len() as u32;
    let queried = unsafe {
        QueryFullProcessImageNameW(process, PROCESS_NAME_WIN32, buffer.as_mut_ptr(), &mut size)
    };
    unsafe { CloseHandle(process) };
    if queried == 0 {
        return include_inaccessible;
    }
    let actual = String::from_utf16_lossy(&buffer[..size as usize]);
    image_path_matches(Some(&actual), expected, include_inaccessible)
}

fn image_path_matches(actual: Option<&str>, expected: &Path, include_inaccessible: bool) -> bool {
    actual.map_or(include_inaccessible, |actual| {
        actual.eq_ignore_ascii_case(&expected.to_string_lossy())
    })
}

fn format_agent_status(process_ids: &[u32]) -> String {
    match process_ids {
        [] => "状态：已停止".to_owned(),
        [process_id] => format!("状态：运行中（PID {process_id}）"),
        process_ids => format!(
            "状态：运行中（{} 个进程：PID {}）",
            process_ids.len(),
            process_ids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub fn run() -> Result<()> {
    let options = parse_runtime_options(std::env::args_os().skip(1))?;
    RUNTIME_OPTIONS
        .set(options.clone())
        .map_err(|_| anyhow!("tray runtime options already initialized"))?;

    // A console-subsystem executable is retained for useful foreground agent
    // diagnostics. Hide that console immediately when running only the tray.
    unsafe {
        let console = GetConsoleWindow();
        if !console.is_null() {
            ShowWindow(console, SW_HIDE);
        }
    }

    let mutex_name = wide(if options.user_mode {
        "Local\\probe-rs-tray-user"
    } else {
        "Local\\probe-rs-tray-machine"
    });
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
        &format!(
            "probe-rs {} ({})",
            env!("CARGO_PKG_VERSION"),
            runtime_options().scope_name()
        ),
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

    let options = runtime_options();
    let agent = installed_agent_path();
    let process_ids = agent.and_then(|agent| {
        agent_process_ids(&agent, !options.user_mode)
            .context("failed to inspect probe-rs process state")
    });
    let (status_text, start_enabled, stop_enabled, restart_enabled) = match process_ids {
        Ok(process_ids) => {
            let running = !process_ids.is_empty();
            (
                format_agent_status(&process_ids),
                !running,
                running,
                running,
            )
        }
        Err(_) => ("状态：未知".to_owned(), false, false, false),
    };
    let title = wide(&format!(
        "probe-rs {} · {}",
        env!("CARGO_PKG_VERSION"),
        options.scope_name()
    ));
    let status = wide(&status_text);
    let privilege_note = if options.user_mode {
        ""
    } else {
        "（需要管理员权限）"
    };
    let start = wide(&format!("启动探针{privilege_note}"));
    let stop = wide(&format!("停止探针{privilege_note}"));
    let restart = wide(&format!("重启探针{privilege_note}"));
    let open_config = wide("安全编辑配置（保存前校验）");
    let about = wide("关于 probe-rs");
    let exit = wide("退出托盘（探针继续运行）");
    AppendMenuW(menu, MF_STRING | MF_GRAYED, 0, title.as_ptr());
    AppendMenuW(menu, MF_STRING | MF_GRAYED, 0, status.as_ptr());
    AppendMenuW(menu, MF_SEPARATOR, 0, null());
    AppendMenuW(
        menu,
        enabled_menu_flags(start_enabled),
        MENU_START,
        start.as_ptr(),
    );
    AppendMenuW(
        menu,
        enabled_menu_flags(stop_enabled),
        MENU_STOP,
        stop.as_ptr(),
    );
    AppendMenuW(
        menu,
        enabled_menu_flags(restart_enabled),
        MENU_RESTART,
        restart.as_ptr(),
    );
    AppendMenuW(menu, MF_SEPARATOR, 0, null());
    AppendMenuW(menu, MF_STRING, MENU_OPEN_CONFIG, open_config.as_ptr());
    AppendMenuW(menu, MF_STRING, MENU_ABOUT, about.as_ptr());
    AppendMenuW(menu, MF_SEPARATOR, 0, null());
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
            MENU_START => launch_control(window, ControlAction::Start),
            MENU_STOP => launch_control(window, ControlAction::Stop),
            MENU_RESTART => launch_control(window, ControlAction::Restart),
            MENU_OPEN_CONFIG => launch_control(window, ControlAction::EditConfig),
            MENU_ABOUT => show_about(window),
            MENU_EXIT => {
                DestroyWindow(window);
            }
            _ => {}
        }
    }
    DestroyMenu(menu);
}

fn enabled_menu_flags(enabled: bool) -> u32 {
    MF_STRING | if enabled { 0 } else { MF_GRAYED }
}

unsafe fn launch_control(window: HWND, action: ControlAction) {
    let options = runtime_options();
    let result = if options.user_mode {
        launch_user_control(action, options)
    } else {
        launch_elevated_control(window, action, options)
    };
    if let Err(error) = result {
        show_message(
            window,
            "probe-rs 操作未启动",
            &format!("{error:#}"),
            MB_OK | MB_ICONERROR,
        );
    }
}

fn action_name(action: ControlAction) -> &'static str {
    match action {
        ControlAction::Start => "start",
        ControlAction::Stop => "stop",
        ControlAction::Restart => "restart",
        ControlAction::EditConfig => "edit-config",
    }
}

fn launch_user_control(action: ControlAction, options: &RuntimeOptions) -> Result<()> {
    use std::os::windows::process::CommandExt;

    let executable = std::env::current_exe().context("failed to locate tray executable")?;
    Command::new(&executable)
        .arg("--tray-control")
        .arg(action_name(action))
        .arg("--user-mode")
        .arg("--config")
        .arg(&options.config_path)
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW)
        .spawn()
        .with_context(|| format!("failed to launch {}", executable.display()))?;
    Ok(())
}

unsafe fn launch_elevated_control(
    window: HWND,
    action: ControlAction,
    options: &RuntimeOptions,
) -> Result<()> {
    let executable_path = std::env::current_exe().context("failed to locate tray executable")?;
    let executable = wide_os(executable_path.as_os_str());
    let verb = wide("runas");
    let config = options.config_path.to_string_lossy();
    if config.contains('"') {
        bail!("configuration path contains a quote");
    }
    let parameters = wide(&format!(
        "--tray-control {} --config \"{config}\"",
        action_name(action)
    ));
    let launched = ShellExecuteW(
        window,
        verb.as_ptr(),
        executable.as_ptr(),
        parameters.as_ptr(),
        null(),
        SW_HIDE,
    );
    if launched as isize <= 32 {
        bail!(
            "administrator helper could not be started (UAC may have been cancelled): {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

unsafe fn show_about(window: HWND) {
    let options = runtime_options();
    let runtime = if options.user_mode {
        "当前用户登录启动项；控制与编辑无需管理员权限。\n用户注销后探针停止运行。"
    } else {
        "SYSTEM 计划任务；控制与编辑时请求管理员权限。\n未登录用户时仍可持续运行。"
    };
    show_message(
        window,
        "probe-rs",
        &format!(
            "probe-rs 托盘伴随程序 {}\n构建目标：windows/{}\n安装范围：{}\n配置文件：{}\n\n{}\n退出托盘不会停止探针。",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::ARCH,
            options.scope_name(),
            options.config_path.display(),
            runtime
        ),
        MB_OK,
    );
}

unsafe fn show_message(window: HWND, title: &str, body: &str, flags: u32) -> i32 {
    let title = wide(title);
    let body = wide(body);
    MessageBoxW(window, body.as_ptr(), title.as_ptr(), flags)
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn wide_os(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(std::iter::once(0)).collect()
}

fn copy_wide(target: &mut [u16], value: &str) {
    for (target, value) in target.iter_mut().zip(value.encode_utf16()) {
        *target = value;
    }
}

fn last_error(action: &str) -> anyhow::Error {
    anyhow!("{action}: {}", std::io::Error::last_os_error())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn formats_stopped_single_and_multiple_agent_processes() {
        assert_eq!(format_agent_status(&[]), "状态：已停止");
        assert_eq!(format_agent_status(&[1234]), "状态：运行中（PID 1234）");
        assert_eq!(
            format_agent_status(&[1234, 5678]),
            "状态：运行中（2 个进程：PID 1234, 5678）"
        );
    }

    #[test]
    fn machine_scope_accepts_inaccessible_processes_but_user_scope_does_not() {
        let expected = Path::new(r"C:\Program Files\probe-rs\probe-rs.exe");
        assert!(image_path_matches(None, expected, true));
        assert!(!image_path_matches(None, expected, false));
        assert!(image_path_matches(
            Some(r"c:\program files\PROBE-RS\probe-rs.exe"),
            expected,
            false
        ));
        assert!(!image_path_matches(
            Some(r"C:\Users\demo\probe-rs.exe"),
            expected,
            true
        ));
    }

    #[test]
    fn edit_copy_has_a_notepad_friendly_name_and_original_contents() {
        let root = tempfile::tempdir().unwrap();
        let (edit_dir, edit_path) = create_edit_copy(root.path(), "schema = 1\n").unwrap();

        assert_eq!(
            edit_path.file_name().and_then(OsStr::to_str),
            Some("probe-rs-edit.toml")
        );
        assert!(edit_path.starts_with(edit_dir.path()));
        assert_eq!(std::fs::read_to_string(edit_path).unwrap(), "schema = 1\n");
    }

    #[test]
    fn parses_supported_tray_control_actions() {
        for (name, expected) in [
            ("start", ControlAction::Start),
            ("stop", ControlAction::Stop),
            ("restart", ControlAction::Restart),
            ("edit-config", ControlAction::EditConfig),
            ("open-config", ControlAction::EditConfig),
        ] {
            let request =
                parse_control_request(args(&["--tray-control", name, "--config", "machine.toml"]))
                    .unwrap()
                    .unwrap();
            assert_eq!(request.action, expected);
            assert_eq!(request.options.config_path, PathBuf::from("machine.toml"));
            assert!(!request.options.user_mode);
        }
    }

    #[test]
    fn parses_user_mode_tray_and_control_options() {
        let options =
            parse_runtime_options(args(&["--tray", "--user-mode", "--config=user.toml"])).unwrap();
        assert!(options.user_mode);
        assert_eq!(options.config_path, PathBuf::from("user.toml"));

        let request = parse_control_request(args(&[
            "--tray-control",
            "restart",
            "--user-mode",
            "--config",
            "user.toml",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(request.action, ControlAction::Restart);
        assert_eq!(request.options, options);
    }

    #[test]
    fn ignores_non_control_process_modes() {
        assert_eq!(parse_control_request(args(&["--tray"])).unwrap(), None);
        assert_eq!(
            parse_control_request(args(&["--config", "config.toml"])).unwrap(),
            None
        );
    }

    #[test]
    fn rejects_invalid_tray_control_requests() {
        assert!(parse_control_request(args(&["--tray-control"])).is_err());
        assert!(parse_control_request(args(&["--tray-control", "invalid"])).is_err());
        assert!(parse_control_request(args(&["--tray-control", "start", "extra"])).is_err());
    }
}
