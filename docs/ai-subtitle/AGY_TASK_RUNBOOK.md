# ynoTV 桌面端 AI 实时延时同传 — AGY 任务执行指导书

> **文档定位**：本指南专供在 VS Code / Antigravity 中打开 `D:\ynotv-src` 后，直接向 AGY 下达分步开发指令使用。
> **参考蓝图**：`D:\owntv+\app\src\main\java\tv\own\owntv\features\aisubtitle\`（OwnTV+ 完整成熟代码）
> **项目规范**：[D:\ynotv-src\.agents\skills\ynotv-tauri-ai-subtitle\SKILL.md](file:///D:/ynotv-src/.agents/skills/ynotv-tauri-ai-subtitle/SKILL.md)

---

## 一、开发前置自检清单（在 VS Code 终端验证）

```powershell
# 1. 验证编译工具链
cargo --version       # 应输出 1.99.x
pnpm --version        # 应输出 9.1.0

# 2. 验证 sidecar 与 dll 文件
Get-ChildItem packages\app\src-tauri\bin\
# 必须存在：ffmpeg-x86_64-pc-windows-msvc.exe, mpv-x86_64-pc-windows-msvc.exe, yt-dlp-x86_64-pc-windows-msvc.exe

Get-ChildItem packages\app\src-tauri\libmpv-2.dll, packages\app\src-tauri\vulkan-1.dll
# 必须全部存在

# 3. 运行静态编译检查
cd packages\app\src-tauri
cargo check
cd ..\..\
```

---

## 二、四个阶段的任务提示词（直接复制发给 AGY）

### 阶段一：Rust 后端多模型引擎与数据模型
**给 AGY 的提示词：**
```text
请阅读 docs/ai-subtitle/PLAN.md 和 .agents/skills/ynotv-tauri-ai-subtitle/SKILL.md。
现在开始实施 Phase 1：
1. 在 packages/app/src-tauri/src/ 下创建 ai_subtitle/ 模块目录，包含：
   - models.rs：定义 AiSubtitleConfig、AiSubtitleCue、AiSubtitleState、AiSubtitleProvider、Pcm16kChunk 数据结构（对齐 OwnTV+ 的字段标准）。
   - config.rs：基于 tauri-plugin-store 或本地 JSON 持久化 AI 字幕配置。
   - engine.rs：实现 AiSubtitleEngine trait：
     * 路线 (a)：阿里百炼两步走引擎（Paraformer / SenseVoice ASR + Qwen-Plus/Turbo 带着最近 2 句上下文翻译）。
     * 路线 (b)：百炼 Realtime WebSocket 引擎（支持 qwen3.8-livetranslate-flash-realtime，附带 45 秒主动刷新避免 60s 断流机制）。
     * 路线 (c)：OpenAI 兼容接口（智谱 GLM-4-Flash / Gemini 2.5 Flash / DeepSeek 备用）。
   - test_conn.rs：实现 test_ai_connection 命令，可测试用户填入的 Base URL 和 API Key 的连通性与延迟。
2. 在 lib.rs 中注册相关 Tauri 命令，并在 packages/app/src-tauri 目录下运行 cargo check 确保编译零报错。
```

---

### 阶段二：FFmpeg 超前探路 + WASAPI 环回直采 + 双时钟控制器
**给 AGY 的提示词：**
```text
现在开始实施 Phase 2：
1. 在 packages/app/src-tauri/src/ai_subtitle/ 下创建：
   - audio_scout.rs：
     * 模式 A（默认）：调用 src-tauri/bin/ffmpeg 启动纯音频后台拉流，输出 16kHz Mono s16le PCM（每 3200 字节对应 100ms 帧，计算 RMS 能量并做 VAD 切片）。
     * 模式 B（回退）：与 packages/app/src-tauri/src/audio_capture.rs 的 WASAPI 进程环回采集对接，当单连接源 8 秒建连超时或返回 403 时，自动回退至 WASAPI 直采，零额外网络连接防踢线。
   - controller.rs：
     * 拦截当前播放流，开启时调用 mpv pause 进行 5 秒超前量蓄水，向前端广播倒计时事件；
     * 攒够 leadSeconds 音频后调用 mpv resume 恢复主画面；
     * 维护双时钟（scoutPts vs mainPlayedPts），当 mpv paused_for_cache 时冻结主时钟，防止画面卡顿字幕抢跑；
     * 维护 60 秒环形字幕时间轴，支持 adjust_sync_offset 微调与 sync_to_live_now 立即对齐。
2. 运行 cargo check 验证编译通过。
```

---

### 阶段三：React 前端透明字幕叠加层与播放栏快捷入口
**给 AGY 的提示词：**
```text
现在开始实施 Phase 3：
1. 在 packages/ui/src/components/ 目录下新建 ai-subtitle/：
   - AiSubtitleOverlay.tsx & .css：挂载在 App.tsx 顶层（透明覆盖于视频上方）：
     * 顶部居中显示「✦ AI 字幕准备中 · 正在提前收听并翻译，约 X 秒后开始播放」倒计时胶囊；
     * 底部/新闻抬高(+72px)/顶部渲染双语半透明毛玻璃卡片字幕；
     * 自适应长句字号与平滑翻篇逻辑。
   - AiSubtitleSettingsModal.tsx & .css：提供阿里百炼（两步走/一步走）、智谱、Gemini 一键预设，API Key 输入框，一键连通性测试按钮，以及提前秒数/字号/微调滑块。
2. 修改 packages/ui/src/components/NowPlayingBar.tsx：
   - 在底部播放控制栏右侧添加「✦ AI 同传」快捷开关按钮（激活时显示青色呼吸灯状态）。
3. 运行 pnpm --filter @ynotv/ui typecheck 确保 TypeScript 类型全部通过。
```

---

### 阶段四：全链路实测与验收
**给 AGY 的提示词：**
```text
现在进行 Phase 4 验收：
1. 执行 pnpm --filter @ynotv/app tauri dev 启动应用；
2. 在播放器中打开任意直播流，点击播放栏上的「✦ AI 同传」；
3. 打开设置面板填入百炼 API Key 并点击「测试连接」验证通过；
4. 开启字幕后验证：
   - 画面是否暂停 5 秒并在顶部显示准备倒计时；
   - 恢复播放后字幕是否与语音同时出现；
   - 换台后是否自动跟随新频道重新探路；
   - 模拟卡顿或手动暂停时字幕是否保持冻结。
```
