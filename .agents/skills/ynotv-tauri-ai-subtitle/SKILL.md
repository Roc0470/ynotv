---
name: ynotv-tauri-ai-subtitle
description: >-
  Architectural runbook, build/dev workflow, and AI real-time delayed-stream subtitle implementation guide for ynoTV (Tauri v2 + React 19 + Rust + mpv/FFmpeg). Activate this skill whenever developing, debugging, or building ynoTV desktop features, especially the AI delayed live translation pipeline (Alibaba Bailian DashScope Realtime/OpenAI-compat, Zhipu, Gemini, TVGO), FFmpeg audio scout, WASAPI direct tap, or React overlay UI.
---

# ynoTV (Tauri v2) + AI 实时延时同传开发规范与技术手册

本技能沉淀了 `ynoTV`（源码位于 `D:\ynotv-src`）的构建避坑指南，以及将 `OwnTV+` 的 **AI 实时延时直播同传翻译架构** 移植到 Windows Tauri v2 + Rust + mpv 桌面端的核心标准。

---

## 一、项目路径与日常开发构建铁律

1. **目录拓扑**：
   - **便携版成品目录（严禁破坏）**：`D:\ynoTV`（包含用户现有的 `ynotv.exe` 和 `ynotv.db` 数据，切勿覆盖）。
   - **源码开发主目录**：`D:\ynotv-src`（Monorepo 结构：`packages/app`、`packages/ui`、`packages/core`、`packages/local-adapter`）。
   - **编译工具链路径（均在 D 盘）**：
     - Rust 工具链：`D:\Rust\.cargo\bin` (`CARGO_HOME=D:\Rust\.cargo`, `RUSTUP_HOME=D:\Rust\.rustup`)
     - VS C++ 生成工具：`D:\VSBuildTools`
     - VS Code 便携版：`D:\VSCode`

2. **启动与编译命令铁律**：
   - **禁止在根目录裸跑 `pnpm dev`**：根目录 `pnpm -r --parallel dev` 会同时拉起两个 Vite 实例争抢 `localhost:5173` 端口导致冲突失败。
   - **唯一标准开发启动命令**：
     ```powershell
     cd D:\ynotv-src
     pnpm --filter @ynotv/app tauri dev
     ```
   - **后端 Rust 快速静态编译检查**（修改 `src-tauri` 代码后优先用此命令秒级验错）：
     ```powershell
     cd D:\ynotv-src\packages\app\src-tauri
     cargo check
     ```
   - **前端 TypeScript 类型与构建检查**：
     ```powershell
     cd D:\ynotv-src
     pnpm --filter @ynotv/ui typecheck
     ```

3. **Tauri 构建外部依赖（缺一不可）**：
   - `packages/app/src-tauri/bin/` 下必须保持三大 Sidecar 二进制齐全：
     - `mpv-x86_64-pc-windows-msvc.exe`
     - `ffmpeg-x86_64-pc-windows-msvc.exe`（严禁拷贝 WinGet Links 下的软链接 shim，必须是真实二进制）
     - `yt-dlp-x86_64-pc-windows-msvc.exe`
   - `packages/app/src-tauri/` 根目录下必须保持三大 Windows 动态库与许可文件齐全：
     - `libmpv-2.dll`、`vulkan-1.dll`、`VulkanRT-License.txt`

---

## 二、ynoTV 视频渲染与透明叠加层机制（关键架构）

1. **Windows 窗口透明打孔渲染机制**：
   - `tauri.conf.json` 中主窗口配置为 `"transparent": true, "decorations": false`，且 `.app` 容器背景为 `background: transparent`。
   - `mpv`（无论是默认 `Sidecar` 模式还是 `LibMpv` 模式）通过 `--wid=<HWND>` 嵌入在窗口底层，React WebView2 的 DOM 元素（如 `.now-playing-bar`、`.channel-info-overlay`）直接以透明 HTML/CSS 图层悬浮在视频画面正上方！
2. **为什么 AI 字幕坚持使用 React DOM Overlay 而不走 mpv `sub-add`**：
   - `ynoTV` 在 `mpv_windows.rs` (L647) 和 `mpv_core.rs` (L596) 处理 `file-loaded` 事件时，会自动强制执行 `set_property("sid", "no")` 以防止直播流内置的多条 CC 字幕叠加乱码。
   - 实时流式同传每秒会产生多次 `delta` 增量更新（打字机流式上屏）以及双语对照卡片样式、顶部准备倒计时胶囊（`✦ AI 字幕准备中 · 正在提前收听并翻译，约 X 秒后开始播放`）。
   - 使用 React 组件 `<AiSubtitleOverlay />` 挂载在 `App.tsx` 主视图层（与 `ChannelInfoOverlay` 同级，`z-index: 55`），可 100% 复刻 `OwnTV+` 的半透明毛玻璃卡片、双语对照、新闻抬高避让跑马灯（`+72px`）、顶部体育比分避让、以及全屏/小窗/画中画自适应。

---

## 三、AI 实时延时同传核心架构（Rust 后端 + React 前端）

### 1. 双模式音频采集与超前探路器 (`ai_subtitle/audio_scout.rs`)
复刻 `OwnTV+` 的 `AiAudioScout`，支持三种采集策略（`Auto` / `Standalone` / `DirectTap`）：
- **模式一：FFmpeg 独立超前探路器 (`Standalone` / 默认优先)**
  - 直接复用 `src-tauri/bin/ffmpeg` Sidecar，以无视频纯音频流方式后台拉取当前播放 URL：
    ```text
    ffmpeg -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -user_agent <UA> -headers <Headers> -i <stream_url> -vn -sn -dn -ac 1 -ar 16000 -f s16le -acodec pcm_s16le pipe:1
    ```
  - 每次从 `stdout` 读取 `3200 bytes`（即精确对应 `100ms` 的 16kHz 单声道 16-bit PCM 音频帧），累加 `cumulative_audio_pts_ms += 100`。
  - 同时维护 RMS 均方根能量计算与 `2.0s ~ 3.2s` 语义停顿切片（封装 44 字节 RIFF WAV 头），兼顾流式 WebSocket 与分段 HTTP 接口。
- **模式二：WASAPI 进程环回直采回退 (`DirectTap` / 单连接源防踢保活)**
  - 当直播源有单连接限制（`Max Connections = 1`，FFmpeg 探路器连接返回 403/503 或 8 秒内无首帧产出）或用户强制选择直采时，自动销毁 FFmpeg 探路器，无缝接入 `audio_capture.rs` 已有的 Windows WASAPI 进程级环回（`AUDIOCLIENT_ACTIVATION_TYPE_PROCESS_LOOPBACK`）。
  - 将 `audio_capture.rs` 采集到的 `pcm_mono_buffer`（通常为 48kHz `f32`）通过线性插值重采样为 `16000Hz i16` 字节流直接喂给 AI 引擎，实现**零额外网络连接、100% 防封号防踢线**。

### 2. 延时直播拦截与双时钟对齐控制器 (`ai_subtitle/controller.rs`)
- **阶段 A（首帧探路等待）**：用户开启 AI 字幕后，调用 `mpv` 的 `pause` 暂停主画面，等待 FFmpeg 探路器产出首帧有效 PCM（超时阈值 8 秒，超时自动回退至 WASAPI 直采并恢复播放）。
- **阶段 B（超前量蓄水倒计时）**：探路器跑通后，保持主画面暂停，实时向前端推送 `AiSubtitleState::Preparing { seconds_left, lead_seconds }`（默认 `lead_seconds = 5` 秒），前端在顶部显示倒计时横幅。当 `decoded_audio_ms >= lead_seconds * 1000` 时，自动调用 `mpv` 的 `resume` 恢复主画面播放！
- **阶段 C（双时钟锚定与卡顿保护）**：
  - 维护主播放器真实推进时钟 `main_played_pts_ms`。当监听到 `mpv` 的 `paused_for_cache == true` 或用户手动暂停时，**立即冻结 `main_played_pts_ms`**，防止画面卡顿时字幕继续抢跑。
  - 若主画面卡顿导致 `scout_pts - main_played_pts_ms > target_lead_ms + 6000ms`，在 Windows 下可通过暂停读取探路管道或重同步游标防止无限脱节。
- **阶段 D（60 秒环形时间轴与毫秒级双向微调）**：
  - 维护 `VecDeque<AiSubtitleCue>`（保留最近 60 秒字幕），支持前端快捷键或设置面板实时调节 `sync_offset_ms`（`-10000ms ~ +15000ms`）以及一键 `sync_to_live_now`（立即对齐当前语音）。

### 3. 多模型同传翻译引擎 (`ai_subtitle/engine.rs`)
全面支持阿里百炼（国内直连免代理）及主流大模型：
- **路线 1：阿里百炼 Realtime WebSocket 实时流式同传（超低延迟首选）**
  - 端点：`wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.8-livetranslate-flash-realtime`（或 `qwen-omni-turbo-realtime`）
  - 协议：连接后携带 `Authorization: Bearer <DashScope_Key>` 及 `OpenAI-Beta: realtime=v1`，收到 `session.created` 后发送 `session.update`（配置 `16000Hz PCM`、`speaker_detection`、目标语言 `zh`），每 100ms 发送 `input_audio_buffer.append`（Base64 PCM），实时解析 `conversation.item.input_audio_transcription.*` 与 `response.text.delta` / `response.audio_transcript.delta`。
  - **百炼 60 秒断流保护**：继承 `OwnTV+` 实战经验，每 45 秒在句间边界主动无缝刷新一次 WebSocket 会话，彻底规避百炼服务端 60 秒单次响应流超时断开限制。
- **路线 2：阿里百炼两步走（Paraformer / SenseVoice 语音识别 + Qwen 翻译，高稳健性首选）**
  - 音频切片（2.5s ~ 4.0s WAV，带 RMS 静音过滤与幻觉词黑名单过滤） -> 调百炼 ASR 接口（如 `paraformer-realtime-v2` / `fun-asr` / OpenAI 兼容 `/audio/transcriptions`）转成原文 -> 原文立即上屏（`is_partial = true`）-> 附带最近 2 句上下文调 `https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions`（模型如 `qwen-plus` / `qwen-turbo` / `qwen-mt-turbo`）输出地道简中字幕。
- **路线 3：OpenAI 兼容多模态一步直传（Qwen-Omni / Gemini / 智谱 GLM）**
  - 通过 `/chat/completions` 的 `input_audio`（Base64 WAV）一次请求同时返回【第一行原文 + 第二行译文】，兼容 `qwen-omni-turbo`、`gemini-2.5-flash` 以及智谱 `glm-asr + glm-4-flash`。

### 4. 局域网手机扫码配网扩展 (`web_server.rs`)
- `ynoTV` 的 `src-tauri/src/web_server.rs` 已经内置了基于 `axum` 的局域网服务器（默认端口 `11470`）且前端已安装 `qrcode` 包。
- 直接在现有 `axum::Router` 中挂载 `/ai-config`（手机端 H5 配置页）、`/api/ai-config/test`（一键测试百炼/智谱/Gemini 连通性）与 `/api/ai-config/save`（保存 Key 并同步至桌面端），无需额外新建端口。
