"""
Streamlit：旧版 secid/downSecId 预报方案 → 新项目 `schemes` 配置转换界面。

运行方式（任选其一）：
- 在 hydro_project 根目录：``python -m streamlit run scripts/config_converter_app.py``
- 直接 ``python scripts/config_converter_app.py``：会自动改为用 streamlit 启动（无需手写 streamlit run）
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
CONFIG_DIR = PROJECT_ROOT / "configs"


def _dialog_initial_dir() -> str:
    """浏览对话框默认打开项目 `configs/`（Tk 在 Windows 上要求目录已存在，路径需规范）。"""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    base = CONFIG_DIR if CONFIG_DIR.is_dir() else PROJECT_ROOT
    return os.path.normpath(str(base.resolve()))


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 必须在 import streamlit 之前：用 `python 本脚本` 启动时改为 `streamlit run`，且父进程不加载 streamlit（避免裸跑警告、无页面）
if __name__ == "__main__" and "streamlit" not in sys.modules:
    _script = Path(__file__).resolve()
    os.chdir(PROJECT_ROOT)
    raise SystemExit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(_script),
                "--server.address",
                "127.0.0.1",
                "--server.port",
                "8502",
                "--browser.gatherUsageStats",
                "false",
            ]
        )
    )

import streamlit as st

from hydro_engine.io.project_config_qa import (  # noqa: E402
    ConfigIssue,
    analyze_project_config,
)


def _subprocess_env_for_tk_child() -> dict:
    env = os.environ.copy()
    if sys.platform == "win32":
        env.setdefault("PYTHONUTF8", "1")
    return env


def _run_tk_dialog_python(source: str) -> str:
    """
    在独立子进程中执行 Tk 脚本。Streamlit 回调可能不在主线程，
    在 Windows 上直接调 Tk filedialog 会 TclError / 无响应；子进程有独立主线程可稳定弹窗。
    """
    fd, tmp_path = tempfile.mkstemp(suffix="_hydro_tkdlg.py", text=True)
    os.close(fd)
    path = Path(tmp_path)
    try:
        path.write_text(source, encoding="utf-8")
        r = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
            env=_subprocess_env_for_tk_child(),
        )
        if r.returncode != 0:
            err = (r.stderr or "").strip() or (r.stdout or "").strip()
            raise RuntimeError(err or f"对话框进程退出码 {r.returncode}")
        return (r.stdout or "").strip()
    finally:
        try:
            path.unlink()
        except OSError:
            pass


def _load_convert_legacy() -> Any:
    path = SCRIPTS_DIR / "convert_legacy_config.py"
    spec = importlib.util.spec_from_file_location("convert_legacy_config", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 convert_legacy_config.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.convert_legacy_to_project_config


def _pick_file(title: str) -> Optional[str]:
    init = _dialog_initial_dir()
    src = f"""import tkinter as tk
from tkinter import filedialog
root = tk.Tk()
root.withdraw()
try:
    root.wm_attributes("-topmost", 1)
except tk.TclError:
    pass
p = ""
try:
    p = filedialog.askopenfilename(
        parent=root,
        title={repr(title)},
        initialdir={repr(init)},
        filetypes=[("JSON", "*.json"), ("所有文件", "*.*")],
    ) or ""
finally:
    root.destroy()
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
print(p, end="")
"""
    out = _run_tk_dialog_python(src)
    return out or None


def _pick_dir(title: str) -> Optional[str]:
    init = _dialog_initial_dir()
    src = f"""import tkinter as tk
from tkinter import filedialog
root = tk.Tk()
root.withdraw()
try:
    root.wm_attributes("-topmost", 1)
except tk.TclError:
    pass
p = ""
try:
    p = filedialog.askdirectory(
        parent=root,
        title={repr(title)},
        initialdir={repr(init)},
    ) or ""
finally:
    root.destroy()
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
print(p, end="")
"""
    out = _run_tk_dialog_python(src)
    return out or None


def _issues_to_html(issues: List[ConfigIssue]) -> None:
    err = [i for i in issues if i.severity == "error"]
    warn = [i for i in issues if i.severity == "warning"]
    if err:
        st.error(f"错误 {len(err)} 条")
        for it in err:
            loc = f"（{it.path}）" if it.path else ""
            st.markdown(f"- **{it.message}** {loc}")
    if warn:
        st.warning(f"提醒 {len(warn)} 条")
        for it in warn:
            loc = f"（{it.path}）" if it.path else ""
            st.markdown(f"- {it.message} {loc}")
    if not issues:
        st.success("未发现静态问题，且引擎试加载通过。")


def main() -> None:
    st.set_page_config(page_title="预报方案配置转换", layout="wide")
    pe = st.session_state.pop("picker_error", None)
    if pe:
        st.error(pe)

    # 必须在实例化带 key 的 st.text_input 之前写入，否则报错：
    # session_state.xxx cannot be modified after the widget is instantiated
    if "legacy_path_pending" in st.session_state:
        st.session_state["legacy_path_input"] = st.session_state.pop("legacy_path_pending")
    if "out_dir_pending" in st.session_state:
        st.session_state["out_dir_input"] = st.session_state.pop("out_dir_pending")
    # text_area(key="json_body") 同上：转换结果必须先于控件写入 session_state，否则界面不更新
    if "json_body_pending" in st.session_state:
        st.session_state["json_body"] = st.session_state.pop("json_body_pending")
    if "json_body" not in st.session_state:
        st.session_state["json_body"] = ""

    st.title("旧版 → 新项目预报方案转换")
    st.caption(
        "将旧版 `sections/secList` 结构转为 `metadata` + `schemes[]`；"
        "校验逻辑与 `load_scheme_from_json` 一致，可在下方直接编辑 JSON 后保存。"
    )

    convert_fn = _load_convert_legacy()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("源：旧版方案")
        legacy_path = st.text_input(
            "旧版 JSON 文件路径",
            key="legacy_path_input",
            placeholder=str(PROJECT_ROOT / "legacy.json"),
        )
        b1, b2 = st.columns(2)
        with b1:
            if st.button("浏览选择旧版文件…"):
                try:
                    p = _pick_file("选择旧版预报方案 JSON")
                    if p:
                        st.session_state["legacy_path_pending"] = p
                except Exception as e:
                    st.session_state["picker_error"] = (
                        f"打开文件对话框失败（已改为子进程弹窗，若仍失败请用手动输入路径或下方上传）："
                        f" {type(e).__name__}: {e}"
                    )
                st.rerun()
        with b2:
            up = st.file_uploader("或上传 JSON", type=["json"], key="legacy_upload")
        if up is not None:
            st.session_state["legacy_upload_bytes"] = up.getvalue()
        else:
            st.session_state.pop("legacy_upload_bytes", None)

    with c2:
        st.subheader("目标：新项目配置")
        if "out_dir_input" not in st.session_state:
            st.session_state["out_dir_input"] = str(PROJECT_ROOT / "configs")
        out_dir = st.text_input(
            "保存到的文件夹",
            key="out_dir_input",
        )
        if st.button("浏览选择目标文件夹…"):
            try:
                d = _pick_dir("选择保存新项目配置的文件夹")
                if d:
                    st.session_state["out_dir_pending"] = d
            except Exception as e:
                st.session_state["picker_error"] = (
                    f"打开文件夹对话框失败：{type(e).__name__}: {e}"
                )
            st.rerun()
        out_name = st.text_input("输出文件名", value="forecastSchemeConf_converted.json")

    st.subheader("时间轴（写入 schemes[0].time_axis）")
    tc1, tc2, tc3, tc4 = st.columns(4)
    with tc1:
        time_type = st.selectbox("time_type", ["Hour", "Day", "Minute"], index=0)
    with tc2:
        step_size = st.number_input("step_size", min_value=1, value=1, step=1)
    with tc3:
        warmup_steps = st.number_input("warmup_period_steps", min_value=0, value=0, step=1)
    with tc4:
        forecast_steps = st.number_input("forecast_period_steps", min_value=1, value=24, step=1)
    tc5, tc6 = st.columns(2)
    with tc5:
        correction_steps = st.number_input("correction_period_steps", min_value=0, value=0, step=1)
    with tc6:
        historical_steps = st.number_input(
            "historical_display_period_steps", min_value=0, value=0, step=1
        )

    st.subheader("操作")
    col_run, col_chk, col_save = st.columns(3)
    with col_run:
        do_convert = st.button("转换", type="primary")
    with col_chk:
        do_recheck = st.button("重新校验（按当前 JSON）")
    with col_save:
        do_save = st.button("保存到目标文件夹")

    # --- load legacy ---
    legacy_obj: Any = None
    legacy_err: Optional[str] = None
    if st.session_state.get("legacy_upload_bytes"):
        try:
            legacy_obj = json.loads(
                st.session_state["legacy_upload_bytes"].decode("utf-8")
            )
        except Exception as e:
            legacy_err = f"上传文件解析失败: {e}"
    elif legacy_path.strip():
        lp = Path(legacy_path.strip())
        if lp.is_file():
            try:
                legacy_obj = json.loads(lp.read_text(encoding="utf-8"))
            except Exception as e:
                legacy_err = f"读取失败: {e}"

    if legacy_err:
        st.error(legacy_err)

    if do_convert:
        if legacy_obj is None:
            st.error("请先选择有效的旧版 JSON 文件或上传文件。")
        else:
            try:
                converted = convert_fn(
                    legacy_obj,
                    time_type=time_type,
                    step_size=int(step_size),
                    warmup_steps=int(warmup_steps),
                    correction_steps=int(correction_steps),
                    historical_steps=int(historical_steps),
                    forecast_steps=int(forecast_steps),
                )
                body = json.dumps(converted, ensure_ascii=False, indent=2)
                warmup_dt = datetime(2020, 1, 1, 0, 0, 0)
                st.session_state["json_body_pending"] = body
                st.session_state["last_issues"] = analyze_project_config(
                    converted,
                    time_type=time_type,
                    step_size=int(step_size),
                    warmup_start_time=warmup_dt,
                )
                st.success("已生成新项目配置，并完成校验。")
                st.rerun()
            except Exception as e:
                st.error(f"转换失败: {type(e).__name__}: {e}")

    json_text = st.text_area(
        "新项目配置 JSON（可编辑）",
        height=420,
        key="json_body",
    )

    warmup_dt = datetime(2020, 1, 1, 0, 0, 0)

    if do_recheck:
        try:
            data = json.loads(json_text)
            if not isinstance(data, dict):
                raise ValueError("根对象须为 JSON 对象")
            st.session_state["last_issues"] = analyze_project_config(
                data,
                time_type=time_type,
                step_size=int(step_size),
                warmup_start_time=warmup_dt,
            )
        except Exception as e:
            st.error(f"校验失败（JSON 可能无效）: {type(e).__name__}: {e}")
            st.session_state["last_issues"] = None

    qa: Optional[List[ConfigIssue]] = st.session_state.get("last_issues")
    if qa is not None:
        st.subheader("校验结果")
        _issues_to_html(qa)

    if do_save:
        try:
            data = json.loads(json_text)
        except Exception as e:
            st.error(f"保存失败：JSON 无法解析 — {e}")
        else:
            out_p = Path(out_dir.strip()) / out_name.strip()
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            st.success(f"已保存: {out_p}")

    if not json_text.strip():
        st.info("选择旧版文件并点击「转换」，或粘贴新项目 JSON 后点击「重新校验」。")


if __name__ == "__main__":
    main()
