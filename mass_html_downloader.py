import csv
import importlib
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

APP_TITLE = "Mass HTML Downloader"
DEFAULT_TIMEOUT = 20
DEFAULT_WORKERS = 4
DEFAULT_DELAY = 0.3
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
APP_DATA_DIR = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MassHtmlDownloader"
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
PLAYWRIGHT_BROWSERS_DIR = APP_DATA_DIR / "ms-playwright"
PLAYWRIGHT_BROWSERS_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(PLAYWRIGHT_BROWSERS_DIR))
APP_STATE_FILE = APP_DATA_DIR / "app_state.json"
INSTALL_TIMEOUT_SECONDS = 900
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)

PLAYWRIGHT_AVAILABLE = False
PLAYWRIGHT_IMPORT_ERROR = ""
SYNC_PLAYWRIGHT = None
COMPUTE_DRIVER_EXECUTABLE = None

try:
    from playwright.sync_api import sync_playwright as _sync_playwright
    from playwright._impl._driver import compute_driver_executable as _compute_driver_executable

    SYNC_PLAYWRIGHT = _sync_playwright
    COMPUTE_DRIVER_EXECUTABLE = _compute_driver_executable
    PLAYWRIGHT_AVAILABLE = True
except Exception as playwright_exc:  # pragma: no cover
    PLAYWRIGHT_IMPORT_ERROR = str(playwright_exc)


@dataclass
class DownloadResult:
    url: str
    success: bool
    mode: str
    file_path: str = ""
    status: str = ""
    error: str = ""


class HtmlDownloaderApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1040x760")
        self.root.minsize(940, 680)

        self.log_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.stop_requested = False
        self.is_running = False
        self.download_results: list[DownloadResult] = []
        self.playwright_ready = False
        self.installation_in_progress = False
        self.install_process: subprocess.Popen[str] | None = None
        self.state = self._load_state()

        self.output_dir_var = tk.StringVar(value=str(Path.cwd() / "downloaded_html"))
        self.workers_var = tk.StringVar(value=str(DEFAULT_WORKERS))
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        self.delay_var = tk.StringVar(value=str(DEFAULT_DELAY))
        self.retries_var = tk.StringVar(value="2")
        self.mode_var = tk.StringVar(value="http")
        self.prefix_numbers_var = tk.BooleanVar(value=True)
        self.create_subfolder_var = tk.BooleanVar(value=True)
        self.overwrite_var = tk.BooleanVar(value=False)
        self.headless_var = tk.BooleanVar(value=True)
        self.wait_after_load_var = tk.StringVar(value="1.5")

        self._build_ui()
        self.root.after(120, self._poll_log_queue)
        self.root.after(250, self._run_startup_checks)

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        main = ttk.Frame(self.root, padding=14)
        main.grid(row=0, column=0, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.rowconfigure(2, weight=1)
        main.rowconfigure(4, weight=1)

        settings = ttk.LabelFrame(main, text="Параметры", padding=12)
        settings.grid(row=0, column=0, sticky="ew")
        for col in range(4):
            settings.columnconfigure(col, weight=1 if col in (1, 3) else 0)

        ttk.Label(settings, text="Папка сохранения").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(settings, textvariable=self.output_dir_var).grid(row=0, column=1, columnspan=2, sticky="ew", padx=(8, 8), pady=4)
        ttk.Button(settings, text="Выбрать", command=self.choose_output_dir).grid(row=0, column=3, sticky="e", pady=4)

        mode_frame = ttk.Frame(settings)
        mode_frame.grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 4))
        ttk.Label(mode_frame, text="Режим загрузки:").pack(side="left")
        ttk.Radiobutton(mode_frame, text="HTTP (быстро)", value="http", variable=self.mode_var, command=self._refresh_mode_hint).pack(side="left", padx=(10, 8))
        ttk.Radiobutton(mode_frame, text="Playwright / браузер (JS-сайты)", value="browser", variable=self.mode_var, command=self._refresh_mode_hint).pack(side="left")

        row2 = ttk.Frame(settings)
        row2.grid(row=2, column=0, columnspan=4, sticky="ew", pady=4)
        ttk.Label(row2, text="Потоков").pack(side="left")
        ttk.Entry(row2, textvariable=self.workers_var, width=8).pack(side="left", padx=(8, 18))
        ttk.Label(row2, text="Timeout, сек").pack(side="left")
        ttk.Entry(row2, textvariable=self.timeout_var, width=8).pack(side="left", padx=(8, 18))
        ttk.Label(row2, text="Задержка, сек").pack(side="left")
        ttk.Entry(row2, textvariable=self.delay_var, width=8).pack(side="left", padx=(8, 18))
        ttk.Label(row2, text="Повторы").pack(side="left")
        ttk.Entry(row2, textvariable=self.retries_var, width=8).pack(side="left", padx=(8, 0))

        row3 = ttk.Frame(settings)
        row3.grid(row=3, column=0, columnspan=4, sticky="w", pady=(4, 2))
        ttk.Checkbutton(row3, text="Нумеровать файлы", variable=self.prefix_numbers_var).pack(side="left", padx=(0, 18))
        ttk.Checkbutton(row3, text="Создать подпапку по времени", variable=self.create_subfolder_var).pack(side="left", padx=(0, 18))
        ttk.Checkbutton(row3, text="Перезаписывать существующие", variable=self.overwrite_var).pack(side="left")

        browser_row = ttk.Frame(settings)
        browser_row.grid(row=4, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Checkbutton(browser_row, text="Headless браузер", variable=self.headless_var).pack(side="left", padx=(0, 18))
        ttk.Label(browser_row, text="Ожидание после загрузки, сек").pack(side="left")
        ttk.Entry(browser_row, textvariable=self.wait_after_load_var, width=8).pack(side="left", padx=(8, 18))
        self.install_browser_button = ttk.Button(browser_row, text="Проверить / установить Chromium", command=self.install_browser_dependencies)
        self.install_browser_button.pack(side="left")
        self.copy_install_command_button = ttk.Button(browser_row, text="Скопировать команду установки", command=self.copy_install_command)
        self.copy_install_command_button.pack(side="left", padx=(8, 0))

        self.mode_hint_label = ttk.Label(settings, text="", foreground="#666666")
        self.mode_hint_label.grid(row=5, column=0, columnspan=4, sticky="w", pady=(8, 0))
        self._refresh_mode_hint()

        actions = ttk.Frame(main)
        actions.grid(row=1, column=0, sticky="ew", pady=(10, 10))
        ttk.Button(actions, text="Импорт TXT/CSV", command=self.import_urls).pack(side="left")
        ttk.Button(actions, text="Открыть папку результата", command=self.open_output_dir).pack(side="left", padx=(8, 0))
        ttk.Button(actions, text="Очистить список", command=self.clear_urls).pack(side="left", padx=(8, 0))
        self.stop_button = ttk.Button(actions, text="Стоп", command=self.request_stop, state="disabled")
        self.stop_button.pack(side="right")
        self.start_button = ttk.Button(actions, text="Старт", command=self.start_download)
        self.start_button.pack(side="right", padx=(0, 8))

        urls_frame = ttk.LabelFrame(main, text="Ссылки — по одной на строку", padding=8)
        urls_frame.grid(row=2, column=0, sticky="nsew")
        urls_frame.columnconfigure(0, weight=1)
        urls_frame.rowconfigure(1, weight=1)

        urls_tools = ttk.Frame(urls_frame)
        urls_tools.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        self.paste_urls_button = ttk.Button(urls_tools, text="Вставить из буфера", command=self.paste_urls_from_clipboard)
        self.paste_urls_button.pack(side="left")
        self.remove_selected_button = ttk.Button(urls_tools, text="Удалить выделенное", command=self.delete_selected_urls)
        self.remove_selected_button.pack(side="left", padx=(8, 0))
        self.save_urls_button = ttk.Button(urls_tools, text="Сохранить список в TXT", command=self.export_urls_to_txt)
        self.save_urls_button.pack(side="left", padx=(8, 0))
        self.urls_count_label = ttk.Label(urls_tools, text="Ссылок: 0")
        self.urls_count_label.pack(side="right")

        self.urls_text = tk.Text(urls_frame, wrap="none", font=("Consolas", 10), relief="flat", borderwidth=0, undo=True)
        self.urls_text.grid(row=1, column=0, sticky="nsew")
        urls_scroll = ttk.Scrollbar(urls_frame, orient="vertical", command=self.urls_text.yview)
        urls_scroll.grid(row=1, column=1, sticky="ns")
        self.urls_text.configure(yscrollcommand=urls_scroll.set)
        self.urls_text.bind("<KeyRelease>", self._on_urls_text_changed)
        self.urls_text.bind("<<Paste>>", self._on_urls_text_changed)
        self.urls_text.bind("<<Cut>>", self._on_urls_text_changed)
        self.urls_text.bind("<<Undo>>", self._on_urls_text_changed)
        self.urls_text.bind("<<Redo>>", self._on_urls_text_changed)
        self._build_urls_context_menu()

        progress_frame = ttk.LabelFrame(main, text="Прогресс", padding=12)
        progress_frame.grid(row=3, column=0, sticky="ew", pady=(10, 10))
        progress_frame.columnconfigure(0, weight=1)
        self.progress = ttk.Progressbar(progress_frame, mode="determinate")
        self.progress.grid(row=0, column=0, sticky="ew")
        self.progress_label = ttk.Label(progress_frame, text="Ожидание запуска")
        self.progress_label.grid(row=1, column=0, sticky="w", pady=(8, 0))

        log_frame = ttk.LabelFrame(main, text="Лог", padding=8)
        log_frame.grid(row=4, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, wrap="word", font=("Consolas", 10), state="disabled", relief="flat", borderwidth=0)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _load_state(self) -> dict:
        if APP_STATE_FILE.exists():
            try:
                return json.loads(APP_STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_state(self) -> None:
        APP_STATE_FILE.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")

    def _run_startup_checks(self) -> None:
        self._append_log(f"Playwright browsers path: {PLAYWRIGHT_BROWSERS_DIR}")
        self._refresh_playwright_status(log_result=True)

        if self.state.get("browser_dependencies_checked_once"):
            return

        self.state["browser_dependencies_checked_once"] = True
        self._save_state()

        if self.playwright_ready:
            return

        if PLAYWRIGHT_AVAILABLE:
            install_now = messagebox.askyesno(
                "Установка Chromium",
                "Для браузерного режима нужен Chromium для Playwright.\n\n"
                "Программа может сама проверить и установить его в первый запуск.\n\n"
                "Установить сейчас?",
            )
            if install_now:
                self.install_browser_dependencies()
            return

        if not getattr(sys, "frozen", False):
            install_package = messagebox.askyesno(
                "Установка Playwright",
                "Python-пакет playwright не найден.\n\n"
                "Программа может попытаться установить его автоматически.\n\n"
                "Установить сейчас?",
            )
            if install_package:
                self.install_browser_dependencies()

    def _refresh_mode_hint(self) -> None:
        if self.mode_var.get() == "http":
            text = "HTTP — быстрый режим. Подходит для обычных страниц, где HTML доступен сразу в ответе сервера."
        else:
            if self.playwright_ready:
                text = f"Playwright готов. Chromium найден. Путь браузеров: {PLAYWRIGHT_BROWSERS_DIR}"
            elif PLAYWRIGHT_AVAILABLE:
                text = (
                    "Playwright импортируется, но Chromium для браузерного режима не найден. "
                    "Нажмите 'Проверить / установить Chromium'."
                )
            else:
                text = (
                    "Playwright пока недоступен. Для режима браузера программа может попытаться установить зависимость автоматически "
                    "или можно выполнить: python -m pip install playwright"
                )
        self.mode_hint_label.configure(text=text)

    def _refresh_playwright_status(self, log_result: bool = False) -> bool:
        self.playwright_ready = False
        if not PLAYWRIGHT_AVAILABLE or SYNC_PLAYWRIGHT is None:
            if log_result:
                self._append_log(f"Playwright import error: {PLAYWRIGHT_IMPORT_ERROR}")
            self._refresh_mode_hint()
            return False

        try:
            with SYNC_PLAYWRIGHT() as playwright:
                browser = playwright.chromium.launch(headless=True)
                browser.close()
            self.playwright_ready = True
            if log_result:
                self._append_log("Playwright: Chromium доступен.")
        except Exception as exc:
            if log_result:
                self._append_log(f"Playwright: Chromium недоступен. {exc}")
        self._refresh_mode_hint()
        return self.playwright_ready

    def choose_output_dir(self) -> None:
        folder = filedialog.askdirectory(title="Выберите папку для сохранения HTML")
        if folder:
            self.output_dir_var.set(folder)

    def open_output_dir(self) -> None:
        path = Path(self.output_dir_var.get().strip())
        path.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(path)  # type: ignore[attr-defined]
        except Exception:
            self._append_log(f"Папка результата: {path}")

    def copy_install_command(self) -> None:
        command = self._get_manual_install_command()
        self.root.clipboard_clear()
        self.root.clipboard_append(command)
        self._append_log("Команда установки Chromium скопирована в буфер обмена.")

    def install_browser_dependencies(self) -> None:
        if self.installation_in_progress:
            return
        self.installation_in_progress = True
        self.install_browser_button.configure(state="disabled")
        self.copy_install_command_button.configure(state="disabled")
        self._append_log("Запущена проверка / установка зависимостей для браузерного режима...")
        threading.Thread(target=self._install_browser_dependencies_worker, daemon=True).start()

    def _install_browser_dependencies_worker(self) -> None:
        try:
            if not PLAYWRIGHT_AVAILABLE:
                if getattr(sys, "frozen", False):
                    self.log_queue.put(("log", "Текущая exe-сборка не содержит playwright. Нужна пересборка с включёнными модулями playwright."))
                    return

                self.log_queue.put(("log", "Playwright не найден. Пытаюсь установить python-пакет..."))
                completed = subprocess.run(
                    [sys.executable, "-m", "pip", "install", "playwright"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    creationflags=CREATE_NO_WINDOW,
                )
                if completed.stdout.strip():
                    self.log_queue.put(("log", completed.stdout.strip()))
                if completed.returncode != 0:
                    error_text = completed.stderr.strip() or "Не удалось установить playwright."
                    self.log_queue.put(("log", error_text))
                    return

                self.log_queue.put(("log", "Python-пакет playwright установлен. Перезагружаю модуль..."))
                self._reload_playwright_module()
                if not PLAYWRIGHT_AVAILABLE:
                    self.log_queue.put(("log", f"Playwright по-прежнему недоступен: {PLAYWRIGHT_IMPORT_ERROR}"))
                    return

            self.log_queue.put(("log", f"Устанавливаю Chromium в {PLAYWRIGHT_BROWSERS_DIR} ..."))
            self.log_queue.put(("log", "Окно консоли появляться не должно. Если установка затянулась, используйте кнопку копирования команды и выполните её вручную."))

            install_result = self._run_chromium_install_process()
            if not install_result:
                return

            if self._refresh_playwright_status():
                self.log_queue.put(("log", "Chromium успешно установлен. Браузерный режим готов к работе."))
            else:
                self.log_queue.put(("log", "Установка завершилась, но Chromium всё ещё недоступен. Проверьте лог выше."))
        except Exception as exc:
            self.log_queue.put(("log", f"Ошибка установки зависимостей: {exc}"))
        finally:
            self.install_process = None
            self.log_queue.put(("install_done", ""))

    def _run_chromium_install_process(self) -> bool:
        node_path, cli_path = COMPUTE_DRIVER_EXECUTABLE()
        env = os.environ.copy()
        env["PLAYWRIGHT_BROWSERS_PATH"] = str(PLAYWRIGHT_BROWSERS_DIR)

        install_command = [node_path, cli_path, "install", "chromium"]
        self.log_queue.put(("log", f"Команда установки: {' '.join(install_command)}"))

        startupinfo = None
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0

        self.install_process = subprocess.Popen(
            install_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            creationflags=CREATE_NO_WINDOW,
            startupinfo=startupinfo,
        )

        assert self.install_process.stdout is not None
        start_time = time.time()
        last_output_time = time.time()
        current_line = []

        while True:
            if self.install_process.poll() is not None:
                tail = self.install_process.stdout.read()
                if tail:
                    for line in tail.splitlines():
                        stripped = line.strip()
                        if stripped:
                            self.log_queue.put(("log", stripped))
                break

            char = self.install_process.stdout.read(1)
            if char:
                current_line.append(char)
                last_output_time = time.time()
                if char == "\n":
                    line = ''.join(current_line).strip()
                    current_line.clear()
                    if line:
                        self.log_queue.put(("log", line))
            else:
                time.sleep(0.1)

            elapsed = time.time() - start_time
            if int(elapsed) % 5 == 0:
                browser_dir_size_mb = self._get_directory_size_mb(PLAYWRIGHT_BROWSERS_DIR)
                self.log_queue.put(("install_progress", f"Идёт установка Chromium... {int(elapsed)} сек, размер папки: {browser_dir_size_mb:.1f} MB"))
                time.sleep(1)

            if time.time() - last_output_time > 120:
                browser_dir_size_mb = self._get_directory_size_mb(PLAYWRIGHT_BROWSERS_DIR)
                self.log_queue.put(("log", f"Установщик не выводит новых строк уже 120 сек. Текущий размер папки браузеров: {browser_dir_size_mb:.1f} MB"))
                self.log_queue.put(("log", f"Можно выполнить вручную: {self._get_manual_install_command()}"))
                self.install_process.terminate()
                self.install_process.wait(timeout=10)
                return False

            if time.time() - start_time > INSTALL_TIMEOUT_SECONDS:
                self.log_queue.put(("log", f"Установка прервана по таймауту {INSTALL_TIMEOUT_SECONDS} сек."))
                self.log_queue.put(("log", f"Можно выполнить вручную: {self._get_manual_install_command()}"))
                self.install_process.terminate()
                self.install_process.wait(timeout=10)
                return False

        if current_line:
            line = ''.join(current_line).strip()
            if line:
                self.log_queue.put(("log", line))

        returncode = self.install_process.returncode
        if returncode != 0:
            self.log_queue.put(("log", f"Установка Chromium завершилась с кодом {returncode}."))
            self.log_queue.put(("log", f"Для ручной установки выполните: {self._get_manual_install_command()}"))
            return False
        return True

    def _get_manual_install_command(self) -> str:
        if getattr(sys, 'frozen', False):
            driver_dir = PLAYWRIGHT_BROWSERS_DIR.parent
            return f"set PLAYWRIGHT_BROWSERS_PATH={PLAYWRIGHT_BROWSERS_DIR} && python -m playwright install chromium"
        return f"set PLAYWRIGHT_BROWSERS_PATH={PLAYWRIGHT_BROWSERS_DIR} && {sys.executable} -m playwright install chromium"

    @staticmethod
    def _get_directory_size_mb(path: Path) -> float:
        total = 0
        if not path.exists():
            return 0.0
        for root, _, files in os.walk(path):
            for name in files:
                try:
                    total += (Path(root) / name).stat().st_size
                except OSError:
                    pass
        return total / (1024 * 1024)

    def _reload_playwright_module(self) -> None:
        global PLAYWRIGHT_AVAILABLE, PLAYWRIGHT_IMPORT_ERROR, SYNC_PLAYWRIGHT, COMPUTE_DRIVER_EXECUTABLE
        try:
            importlib.invalidate_caches()
            sync_module = importlib.import_module("playwright.sync_api")
            driver_module = importlib.import_module("playwright._impl._driver")
            SYNC_PLAYWRIGHT = getattr(sync_module, "sync_playwright")
            COMPUTE_DRIVER_EXECUTABLE = getattr(driver_module, "compute_driver_executable")
            PLAYWRIGHT_AVAILABLE = True
            PLAYWRIGHT_IMPORT_ERROR = ""
        except Exception as exc:
            PLAYWRIGHT_AVAILABLE = False
            PLAYWRIGHT_IMPORT_ERROR = str(exc)

    def _build_urls_context_menu(self) -> None:
        self.urls_context_menu = tk.Menu(self.root, tearoff=0)
        self.urls_context_menu.add_command(label="Вырезать", command=lambda: self._event_generate_safe("<<Cut>>"))
        self.urls_context_menu.add_command(label="Копировать", command=lambda: self._event_generate_safe("<<Copy>>"))
        self.urls_context_menu.add_command(label="Вставить", command=self.paste_urls_from_clipboard)
        self.urls_context_menu.add_separator()
        self.urls_context_menu.add_command(label="Удалить выделенное", command=self.delete_selected_urls)
        self.urls_context_menu.add_command(label="Выделить всё", command=self._select_all_urls)
        self.urls_text.bind("<Button-3>", self._show_urls_context_menu)
        self.urls_text.bind("<Control-a>", self._select_all_urls)
        self._update_urls_count()

    def _show_urls_context_menu(self, event: tk.Event) -> str:
        self.urls_context_menu.tk_popup(event.x_root, event.y_root)
        return "break"

    def _select_all_urls(self, event: tk.Event | None = None) -> str:
        self.urls_text.focus_set()
        self.urls_text.tag_add("sel", "1.0", "end-1c")
        return "break"

    def _event_generate_safe(self, sequence: str) -> None:
        self.urls_text.focus_set()
        self.urls_text.event_generate(sequence)
        self._update_urls_count()

    def _on_urls_text_changed(self, event: tk.Event | None = None) -> None:
        self.root.after(10, self._update_urls_count)

    def _update_urls_count(self) -> None:
        count = len(self._parse_urls(self.urls_text.get("1.0", "end")))
        self.urls_count_label.configure(text=f"Ссылок: {count}")

    def paste_urls_from_clipboard(self) -> None:
        try:
            text = self.root.clipboard_get()
        except tk.TclError:
            messagebox.showwarning("Буфер обмена пуст", "Не удалось получить текст из буфера обмена.")
            return

        payload = text.strip()
        if not payload:
            messagebox.showwarning("Буфер обмена пуст", "В буфере обмена нет текста со ссылками.")
            return

        current = self.urls_text.get("1.0", "end").strip()
        self.urls_text.focus_set()
        if current:
            self.urls_text.insert("end", "\n" + payload)
        else:
            self.urls_text.insert("1.0", payload)
        self._update_urls_count()
        self._append_log("Ссылки вставлены из буфера обмена.")

    def delete_selected_urls(self) -> None:
        try:
            self.urls_text.delete("sel.first", "sel.last")
            self._update_urls_count()
        except tk.TclError:
            messagebox.showinfo("Нет выделения", "Сначала выделите текст, который нужно удалить.")

    def export_urls_to_txt(self) -> None:
        file_path = filedialog.asksaveasfilename(
            title="Сохранить список ссылок",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not file_path:
            return

        text = self.urls_text.get("1.0", "end").strip()
        Path(file_path).write_text(text + ("\n" if text else ""), encoding="utf-8")
        self._append_log(f"Список ссылок сохранён: {file_path}")

    def import_urls(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Выберите файл со ссылками",
            filetypes=[("Text/CSV files", "*.txt *.csv"), ("All files", "*.*")],
        )
        if not file_path:
            return

        try:
            urls = self._read_urls_from_file(file_path)
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось прочитать файл:\n{exc}")
            return

        if not urls:
            messagebox.showwarning("Пустой файл", "Ссылки не найдены.")
            return

        current = self.urls_text.get("1.0", "end").strip()
        payload = "\n".join(urls)
        if current:
            self.urls_text.insert("end", "\n" + payload)
        else:
            self.urls_text.insert("1.0", payload)
        self._append_log(f"Импортировано ссылок: {len(urls)}")

    def clear_urls(self) -> None:
        self.urls_text.delete("1.0", "end")
        self._update_urls_count()

    def request_stop(self) -> None:
        self.stop_requested = True
        self._append_log("Остановка запрошена. Уже начатые загрузки завершатся, новые — не стартуют.")

    def start_download(self) -> None:
        if self.is_running:
            return

        urls = self._parse_urls(self.urls_text.get("1.0", "end"))
        if not urls:
            messagebox.showwarning("Нет ссылок", "Добавьте хотя бы одну ссылку.")
            return

        try:
            workers = max(1, int(self.workers_var.get().strip()))
            timeout = max(1, int(self.timeout_var.get().strip()))
            retries = max(0, int(self.retries_var.get().strip()))
            delay = max(0.0, float(self.delay_var.get().strip().replace(",", ".")))
            wait_after_load = max(0.0, float(self.wait_after_load_var.get().strip().replace(",", ".")))
        except ValueError:
            messagebox.showerror("Ошибка", "Проверьте числовые параметры.")
            return

        if self.mode_var.get() == "browser":
            if self.installation_in_progress:
                messagebox.showinfo("Установка ещё идёт", "Дождитесь завершения установки Chromium.")
                return
            if not self._refresh_playwright_status(log_result=True):
                install_now = messagebox.askyesno(
                    "Chromium не установлен",
                    "Для браузерного режима не найден Chromium для Playwright.\n\n"
                    "Установить автоматически сейчас?",
                )
                if install_now:
                    self.install_browser_dependencies()
                return

        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            messagebox.showerror("Ошибка", "Укажите папку сохранения.")
            return

        self.stop_requested = False
        self.is_running = True
        self.download_results.clear()
        self.progress["maximum"] = len(urls)
        self.progress["value"] = 0
        self.progress_label.configure(text=f"Подготовка: 0 / {len(urls)}")
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")

        threading.Thread(
            target=self._download_worker,
            args=(urls, output_dir, workers, timeout, retries, delay, wait_after_load, self.mode_var.get()),
            daemon=True,
        ).start()

    def _download_worker(
        self,
        urls: list[str],
        output_dir: str,
        workers: int,
        timeout: int,
        retries: int,
        delay: float,
        wait_after_load: float,
        mode: str,
    ) -> None:
        started_at = time.strftime("%Y%m%d_%H%M%S")
        target_dir = Path(output_dir)
        if self.create_subfolder_var.get():
            target_dir = target_dir / f"html_batch_{started_at}"
        target_dir.mkdir(parents=True, exist_ok=True)

        self.log_queue.put(("log", f"Режим: {mode}"))
        self.log_queue.put(("log", f"Папка выгрузки: {target_dir}"))
        self.log_queue.put(("log", f"Ссылок к обработке: {len(urls)}"))

        completed = 0
        futures_map = {}

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for index, url in enumerate(urls, start=1):
                    if self.stop_requested:
                        break
                    futures_map[executor.submit(
                        self._download_single,
                        index,
                        url,
                        target_dir,
                        timeout,
                        retries,
                        delay,
                        wait_after_load,
                        mode,
                    )] = url

                for future in as_completed(futures_map):
                    result = future.result()
                    self.download_results.append(result)
                    completed += 1
                    self.log_queue.put(("progress", f"{completed}|{len(urls)}"))

                    if result.success:
                        self.log_queue.put(("log", f"OK   | [{result.mode}] {result.url} -> {result.file_path}"))
                    else:
                        self.log_queue.put(("log", f"FAIL | [{result.mode}] {result.url} -> {result.error}"))

                    if self.stop_requested:
                        self.log_queue.put(("log", "Остановка подтверждена."))
                        break
        except Exception as exc:
            self.log_queue.put(("log", f"Критическая ошибка: {exc}"))
        finally:
            report_path = self._write_report_csv(target_dir)
            ok_count = sum(1 for item in self.download_results if item.success)
            fail_count = len(self.download_results) - ok_count
            self.log_queue.put(("log", f"CSV-отчёт сохранён: {report_path}"))
            self.log_queue.put(("done", f"Готово. Успешно: {ok_count}, ошибок: {fail_count}"))

    def _download_single(
        self,
        index: int,
        url: str,
        target_dir: Path,
        timeout: int,
        retries: int,
        delay: float,
        wait_after_load: float,
        mode: str,
    ) -> DownloadResult:
        if self.stop_requested:
            return DownloadResult(url=url, success=False, mode=mode, error="Остановлено пользователем")

        file_name = self._build_file_name(index, url)
        file_path = target_dir / file_name

        if file_path.exists() and not self.overwrite_var.get():
            return DownloadResult(url=url, success=True, mode=mode, file_path=str(file_path), status="skipped_exists")

        last_error = "Неизвестная ошибка"
        for attempt in range(retries + 1):
            if self.stop_requested:
                return DownloadResult(url=url, success=False, mode=mode, error="Остановлено пользователем")
            try:
                if delay > 0:
                    time.sleep(delay)

                if mode == "http":
                    html, status = self._fetch_via_http(url, timeout)
                else:
                    html, status = self._fetch_via_browser(url, timeout, wait_after_load)

                file_path.write_text(html, encoding="utf-8", newline="")
                return DownloadResult(url=url, success=True, mode=mode, file_path=str(file_path), status=status)
            except Exception as exc:
                last_error = str(exc)
                if attempt < retries:
                    time.sleep(1.0)

        return DownloadResult(url=url, success=False, mode=mode, error=last_error)

    def _fetch_via_http(self, url: str, timeout: int) -> tuple[str, str]:
        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru,en;q=0.9",
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                status = str(getattr(response, "status", 200))
                raw = response.read()
                content_type = response.headers.get("Content-Type", "")
                encoding = response.headers.get_content_charset() or "utf-8"
            text = raw.decode(encoding, errors="replace")
            if "html" not in content_type.lower() and "<html" not in text.lower():
                raise ValueError(f"Ответ не похож на HTML. Content-Type: {content_type}")
            return text, status
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.reason}") from exc
        except URLError as exc:
            raise RuntimeError(f"URL error: {exc.reason}") from exc

    def _fetch_via_browser(self, url: str, timeout: int, wait_after_load: float) -> tuple[str, str]:
        if not self.playwright_ready:
            raise RuntimeError("Chromium для Playwright не установлен. Нажмите 'Проверить / установить Chromium'.")
        timeout_ms = timeout * 1000
        with SYNC_PLAYWRIGHT() as playwright:
            browser = playwright.chromium.launch(headless=self.headless_var.get())
            page = browser.new_page(user_agent=USER_AGENT)
            try:
                response = page.goto(url, wait_until="networkidle", timeout=timeout_ms)
                if wait_after_load > 0:
                    page.wait_for_timeout(int(wait_after_load * 1000))
                html = page.content()
                status = str(response.status if response else "browser")
                return html, status
            finally:
                browser.close()

    def _build_file_name(self, index: int, url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc or "site"
        path = parsed.path.strip("/") or "index"
        query = parsed.query.strip()

        parts = [host, path.replace("/", "_")]
        if query:
            parts.append(query.replace("&", "_").replace("=", "-"))

        stem = "__".join(part for part in parts if part)
        stem = re.sub(r"[^A-Za-zА-Яа-я0-9._-]+", "_", stem)
        stem = re.sub(r"_+", "_", stem).strip("._")
        stem = stem[:180] if stem else "page"
        if not stem.lower().endswith(".html"):
            stem += ".html"
        return f"{index:04d}_{stem}" if self.prefix_numbers_var.get() else stem

    def _write_report_csv(self, target_dir: Path) -> Path:
        report_path = target_dir / "download_report.csv"
        with report_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle, delimiter=";")
            writer.writerow(["url", "mode", "success", "status", "file_path", "error"])
            for item in self.download_results:
                writer.writerow([item.url, item.mode, item.success, item.status, item.file_path, item.error])
        return report_path

    def _read_urls_from_file(self, file_path: str) -> list[str]:
        ext = Path(file_path).suffix.lower()
        urls: list[str] = []
        if ext == ".csv":
            with open(file_path, "r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.reader(handle):
                    for cell in row:
                        urls.extend(self._parse_urls(cell))
        else:
            with open(file_path, "r", encoding="utf-8-sig") as handle:
                urls = self._parse_urls(handle.read())
        return urls

    @staticmethod
    def _parse_urls(text: str) -> list[str]:
        items = []
        seen = set()
        for raw in text.splitlines():
            url = raw.strip()
            if not url or not re.match(r"^https?://", url, flags=re.IGNORECASE):
                continue
            if url not in seen:
                seen.add(url)
                items.append(url)
        return items

    def _poll_log_queue(self) -> None:
        try:
            while True:
                kind, payload = self.log_queue.get_nowait()
                if kind == "log":
                    self._append_log(payload)
                elif kind == "progress":
                    done, total = payload.split("|")
                    self.progress["value"] = int(done)
                    self.progress_label.configure(text=f"Обработано: {done} / {total}")
                elif kind == "done":
                    self._append_log(payload)
                    self.progress_label.configure(text=payload)
                    self.is_running = False
                    self.start_button.configure(state="normal")
                    self.stop_button.configure(state="disabled")
                elif kind == "install_done":
                    self.installation_in_progress = False
                    self.install_browser_button.configure(state="normal")
                    self.copy_install_command_button.configure(state="normal")
                    self._refresh_playwright_status(log_result=True)
                elif kind == "install_progress":
                    self.progress_label.configure(text=payload)
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._poll_log_queue)

    def _append_log(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")


def main() -> None:
    root = tk.Tk()
    try:
        ttk.Style().theme_use("clam")
    except tk.TclError:
        pass
    HtmlDownloaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
