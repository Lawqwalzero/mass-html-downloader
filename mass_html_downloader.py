import csv
import os
import queue
import re
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
PLAYWRIGHT_AVAILABLE = False
PLAYWRIGHT_IMPORT_ERROR = ""

try:
    from playwright.sync_api import sync_playwright
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
        ttk.Entry(browser_row, textvariable=self.wait_after_load_var, width=8).pack(side="left", padx=(8, 0))

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
        urls_frame.rowconfigure(0, weight=1)

        self.urls_text = tk.Text(urls_frame, wrap="none", font=("Consolas", 10), relief="flat", borderwidth=0)
        self.urls_text.grid(row=0, column=0, sticky="nsew")
        urls_scroll = ttk.Scrollbar(urls_frame, orient="vertical", command=self.urls_text.yview)
        urls_scroll.grid(row=0, column=1, sticky="ns")
        self.urls_text.configure(yscrollcommand=urls_scroll.set)

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

    def _refresh_mode_hint(self) -> None:
        if self.mode_var.get() == "http":
            text = "HTTP — быстрый режим. Подходит для обычных страниц, где HTML доступен сразу в ответе сервера."
        else:
            if PLAYWRIGHT_AVAILABLE:
                text = "Playwright — медленнее, но подходит для страниц, где контент появляется только после выполнения JavaScript."
            else:
                text = "Playwright пока недоступен. Для браузерного режима установите playwright: pip install playwright && playwright install"
        self.mode_hint_label.configure(text=text)

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

        if self.mode_var.get() == "browser" and not PLAYWRIGHT_AVAILABLE:
            messagebox.showerror(
                "Playwright не установлен",
                "Для браузерного режима выполните:\n\npip install playwright\nplaywright install\n\nПосле этого перезапустите утилиту.",
            )
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
        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError(f"Playwright недоступен: {PLAYWRIGHT_IMPORT_ERROR}")
        timeout_ms = timeout * 1000
        with sync_playwright() as playwright:
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
