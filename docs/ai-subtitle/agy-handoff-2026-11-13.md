# ynoTV AI 实时同传字幕：交接摘要

日期：2026-11-13  
项目：`D:\ynotv-src`（Tauri v2 + React 19 + Rust + mpv）  
目标：修复 AI 实时同传字幕的显示时序（不显示、提前、滞后、中文闪没、旧句跳回、整体慢 1～2 秒）。

## 0. 一句话结论

现在剩下的 1～2 秒不是单一“偏移量”问题，而是两个独立延迟叠加：

1. **译文定稿延迟**：只显示“完整、打磨好”的中文时，译文必然在句子音频被消费完之后才产生，约晚 1～1.5 秒；
2. **时钟映射误差**：当前实际走的是 WASAPI 直采回退路径，采集时钟比播放器时钟快 0.1～2.4 秒，且只在会话开始时校准一次，之后不再对齐。

补偿滑块只能移动“显示窗口”，不能让还没生成的文字提前出现，所以按 `[` 调到 -6 秒也没用。

## 1. 显示规则（用户已确认）

- 原文 1 行 + 译文 2 行；一句一条；不缩字号。
- 以“7 秒 / 70 字强制切条”为主，2 行截断兜底。
- 中文必须**完整、打磨好**再上屏，慢一点可以接受；补偿长一点没关系。
- 早期确认：英文大致同步即可；排队逐句放，每句至少约 2 秒，最多落后 4 秒，超了加速追。
- 最新确认：中文必须完整；不接受“出第一个字就换”的半成品。

## 2. 系统结构与时间戳链路

```text
WASAPI 直采 / FFmpeg 探路器
        ↓ 每 100ms 一帧 PCM
engine.rs  RealtimeCueAssembler（VAD 分句 + 流式译文拼接）
        ↓ AiSubtitleCue { start_pts_ms, end_pts_ms, is_partial }
controller.rs  排片（窗口分配/延长） + select_cue（按画面时钟选句）
        ↓ ai-subtitle:cue 事件
AiSubtitleOverlay.tsx  只负责渲染，不做时间计算
```

关键点：

| 环节 | 用的时间 | 位置 |
|---|---|---|
| 音频帧 PTS | `decoded_audio_ms.fetch_add(100)`，起点是开始采集时播放器的位置 | `audio_scout.rs:17,435` |
| 句子起点 | VAD 检测到开口的 PTS；或上一句释放位置 `latest_audio - 1500ms` | `engine.rs:714-743,767` |
| 句子收尾 | 收到译文终止符 / 达到 70 字 / 180 源字符 / 7 秒时定稿 | `engine.rs:676-712,745` |
| 显示时刻 | `main_played_pts_ms + cue_base_pts_ms`，即“播放器播到这句话的音频位置才上屏” | `controller.rs:1043-1047` |
| 校准 | 会话恢复播放时 `cue_base = decoded_audio - main_played`，**只校准一次** | `controller.rs:542,700,1108` |

`base` 的含义：把采集时钟映射到播放器时钟的固定偏移。`base=0` 表示代码认为两个时钟完全一致。

## 3. 已完成的改动

### 3.1 引擎分句（`engine.rs`）

- 新增 `RealtimeCueAssembler`，把 Realtime 流按句切条，输出 `AiSubtitleCue`。
- `finish_current_cue` 在以下任一条件触发时把 `is_partial` 置为 `false`：
  - 译文出现句末标点（`。！？；…!?;`）且至少 4 个字；
  - 译文达到 70 字；
  - 源文达到 180 字；
  - 句子音频持续达到 7 秒。
- 常量：
  - `REALTIME_CUE_TAIL_MS = 3000`
  - `REALTIME_CUE_MAX_DURATION_MS = 7000`
  - `REALTIME_CUE_MAX_TRANSLATED_CHARS = 70`
  - `REALTIME_CUE_MAX_SOURCE_CHARS = 180`
  - `REALTIME_MIN_SENTENCE_CHARS = 4`
  - `REALTIME_TRANSLATION_LAG_MS = 1500`
  - `REALTIME_SPEECH_END_SILENCE_MS = 500`
  - `UNANCHORED_REALTIME_CUE_LOOKBACK_MS = 1000`

### 3.2 控制器排片（`controller.rs`）

- 仅对 `AiSubtitleRoute::Realtime` 启用 `pace_cue_windows`；`two_stage` 不受影响。
  - 注意：配置文件中 `route` 是 `two_stage`，但 `controller.rs:122-128` 会在模型名包含 `realtime` 时强制改成 `Realtime`。当前模型是 `qwen3.8-livetranslate-flash-realtime`，所以实际走 Realtime 排片。
- `assign_new_cue_window`：
  - `heard_at = max(cue.start + base, render_now)`
  - `start = max(next_display_start, heard_at)`
  - 如果排队落后超过 `MAX_DISPLAY_LAG_MS = 4000`，直接回到 `heard_at`
  - `duration = (1200 + effective_chars * 90).clamp(2000, 6000)`
  - `next_display_start = end`
- `extend_cue_window`：
  - 定稿句如果 `start < render_now`，把 `start` 重新锚到 `render_now`，保证完整阅读时间；
  - 未定稿句窗口最多延长到 `start + MAX_CUE_DISPLAY_MS (6000)`；
  - 定稿句窗口按阅读时长计算，同样受 6 秒上限约束。
- `hold_displayed_cue`：
  - 新句还是 partial 时，把当前已显示句子的 `end` 延长到 `min(render_now + 1500, shown.start + 6000)`，避免新句把上一句顶掉造成空白。
- `select_cue`：
  - 排片模式下**只显示 `is_partial == false` 的定稿句**；
  - 窗口为 `[start + sync_offset, end + sync_offset]`；
  - 超过窗口但不超过 `LATE_CUE_GRACE_MS = 4000` 的句子仍可显示；
  - 同一句不重复上屏（`last_emitted_cue` 去重）。
- `sync_cue_base`：
  - `new_base = scout_pts - main_played`；
  - 排片模式下如果 `new_base > old_base`，把所有已有 cue 窗口和 `next_display_start` 平移 `delta`；
  - 只在恢复播放 / 直采回退启动时调用一次。
- `advance_main_clock`：
  - 只在 `playing && !paused_for_cache` 且 `position >= last_position` 时累加播放器位置增量。
- 常量：
  - `MAX_CUE_HISTORY_MS = 60000`
  - `LATE_CUE_GRACE_MS = 4000`
  - `MIN_CUE_DISPLAY_MS = 2000`
  - `MAX_CUE_DISPLAY_MS = 6000`
  - `MAX_DISPLAY_LAG_MS = 4000`
  - `PARTIAL_REFRESH_MS = 1500`

### 3.3 UI / 设置

- `AiSubtitleOverlay.tsx`：
  - 前端兜底 `CUE_LIFETIME_MS = 7000`；
  - 原文 1 行、译文 2 行由 CSS clamp 控制，不缩字号；
  - 播放时按 `[` / `]` 每次 ±500ms；
  - 焦点在 `input/textarea/select/button/[contenteditable]/[role=dialog]` 时不响应快捷键。
- `AiSubtitleSettingsModal.tsx`：
  - 「字幕延迟补偿（秒）」，范围 ±6 秒，步进 0.1 秒，重置为 0。
- `aiSubtitleTypes.ts`：默认 `syncOffsetMs = 0`。
- `AiSubtitleOverlay.css`：1 行原文 / 2 行译文 clamp。

### 3.4 诊断日志（仍在）

每次上屏：

```text
[AiSubtitleDiag] show id=xxxx audio_pts=.. display_pts=.. base=.. delta=..
```

每次接收 cue：

```text
[AiSubtitleDiag] accept new=.. id=.. partial=.. start=.. end=.. src=.. dst=.. render=.. played=.. base=.. selected=..
```

每 100ms 左右的 tick：

```text
[AiSubtitleDiag] tick render=.. played=.. base=.. cues=.. phase_ready=.. advancing=.. state=..
```

引擎事件：

```text
[AiSubtitleDiag] evt=response.text.done latest=.. len=.. preview=..
```

## 4. 当前运行状态

- 最新构建：`target\debug\ynotv.exe`，2026-11-13 09:56:55；进程 09:56:58 启动。
- `cargo check` 通过；`cargo test --lib ai_subtitle` **51 passed, 0 failed**。
  - `two_stage` 的 mock HTTP 测试偶发 `error sending request for url (http://127.0.0.1:.../v1/audio/transcriptions)`，单跑/重跑即过，属环境抖动。
- 配置文件：`C:\Users\Penn\AppData\Roaming\com.ynotv.app\ynotv_ai_subtitle.json`
  - 当前 `syncOffsetMs = -5000`（用户连续按 `[` 按到 -5 秒）
  - `leadSeconds = 5`
  - `route = two_stage`（运行时被强制成 Realtime）
  - `model = qwen3.8-livetranslate-flash-realtime`
  - `audioSource = auto_scout`
- 代码未提交：`packages/app/src-tauri/src/ai_subtitle/` 整个目录、多个 UI 文件仍是 untracked；工作区还有更早的功能改动（`Cargo.toml`、`audio_capture.rs`、`lib.rs`、`App.tsx`、`NowPlayingBar.*`、`usePlayback.ts`）。

## 5. 实测日志证据（2026-11-13 10:14–10:19）

### 5.1 关键发现：FFmpeg 探路器 3 次全部失败，实际走 WASAPI 直采

```text
[10:14:04] Option extension_picky not found.
[10:14:04] Error opening input file http://nba.3691065.site/play/...
[10:14:04] Error opening input files: Option not found
[10:14:04] Standalone scout attempt 1/3 did not produce its first frame within 8 seconds
[10:14:08] attempt 2/3 ...
[10:14:13] attempt 3/3 ...
```

→ 3 次失败后进入 `start_direct_fallback`，`base` 固定为 0，采集时钟从播放器当前位置开始，按 100ms/帧推进。

### 5.2 `base=0` 时，上屏 delta 为 +1.7～+4.0 秒

```text
show id=67dbb190 audio_pts=213461 display_pts=215165 base=0 delta=1704
show id=ca4d3e94 audio_pts=74174  display_pts=76961  base=0 delta=2787
show id=1d8c1ad2 audio_pts=47298  display_pts=50100  base=0 delta=2802
show id=4e3fcf22 audio_pts=199550 display_pts=203237 base=0 delta=3687
show id=a10d9c78 audio_pts=135669 display_pts=139657 base=0 delta=3988
show id=a10d9c78 audio_pts=135669 display_pts=139673 base=0 delta=4004
```

`delta = display_pts - (audio_pts + base)`，正数表示比这句话的音频位置晚了多少毫秒。也就是说：**在补偿为 0 时，字幕实际晚了 1.7～4.0 秒。**

### 5.3 按 `[` 到 -2.5 秒后，delta 变成负数

```text
show id=7e96e89d audio_pts=171014 display_pts=168635 base=0 delta=-2379
show id=41ee01bc audio_pts=186240 display_pts=183650 base=0 delta=-2590
```

再继续按到 -5 秒：

```text
show id=02b3c340 audio_pts=282463 display_pts=282433 base=0 delta=-30
show id=c8c807bc audio_pts=233650 display_pts=233650 base=0 delta=0
```

但用户仍觉得慢 1～2 秒。原因见第 6 节。

### 5.4 采集时钟比播放器时钟快 0.1～2.4 秒

对每条新 cue，比较 `assign_new_cue_window` 时的 `start` 和 `render`：

| 时间 | cue id | start | render | start - render |
|---|---|---|---|---|
| 10:17:5x | b3bb260f | 226301 | 224258 | +2043 |
| 10:18:0x | 5eeb88dc | 236186 | 236186 | 0 |
| 10:18:1x | 8fcf82b8 | 246778 | 246547 | +231 |
| 10:18:2x | 73b5c217 | 249778 | 247431 | +2347 |
| 10:18:4x | 66b5d506 | 273558 | 271188 | +2370 |
| 10:18:5x | 02b3c340 | 282463 | 282349 | +114 |

正数表示采集时钟已经跑到播放器时钟前面。它**不是单调增长**，会跳变；切换频道后会话重启，两个时钟归零，所以“切换频道又正常”。

### 5.5 译文定稿时间

```text
[10:18:1x] evt=conversation.item.input_audio_transcription.completed latest=240200 len=99
[10:18:1x] evt=response.text.done latest=240200 len=46
[10:18:1x] show id=6fe68b11 audio_pts=239256 display_pts=239256 base=0 delta=0
```

这句话的音频起点是 239256，译文完成事件到达时采集时钟已经到 240200，约晚 **0.9 秒**；而这句话的音频本身还在继续。也就是说，**译文是在句子音频被消费完之后才定稿的**。

## 6. 为什么“提前”按钮没有用

`assign_new_cue_window` 里：

```rust
let heard_at_ms = cue.start_pts_ms
    .saturating_add(inner.cue_base_pts_ms)
    .max(render_now_ms);
let mut start_pts_ms = inner.next_display_start_ms
    .take()
    .unwrap_or(heard_at_ms)
    .max(heard_at_ms);
```

`start` 至少等于 `render_now`（当前画面时钟）。然后 `select_cue` 的条件是：

```rust
cue.start_pts_ms + sync_offset <= render_pts_ms
```

当 `sync_offset` 为负数时，这条 cue 在**被接收的同一刻**就满足条件，立即上屏。因此：

- 负补偿只能让“已经到达的文字”立刻显示，**不能让还没到达的文字提前出现**；
- 继续往负调，只会让 `end + sync_offset` 提前，缩短显示时间，甚至掉进 `LATE_CUE_GRACE_MS` 的“迟到兜底”路径，造成闪烁；
- 用户按到 -5 秒后仍觉得慢，是因为文字到达时间才是真正的瓶颈。

**当前 -5000 的配置是有害的，下一步测试前应重置为 0。**

## 7. 当前判断：两个独立延迟

### A. 译文定稿延迟（结构性，约 1～1.5 秒）

只显示 `is_partial == false` 的定稿句，而引擎要等译文出现句末标点才定稿。模型必须先消费完这句话的音频，再输出完整译文，所以：

```text
句子开口 ── 句子说完 ── 译文定稿 ── 上屏
              ↑ 约 1～1.5 秒
```

这是“中文完整、打磨好”这个要求的直接代价。任何 `syncOffsetMs` 都消不掉它。

### B. 时钟映射误差（可修，约 0.1～2.4 秒）

当前实际走 WASAPI 直采回退：

- 采集时钟：`decoded_audio_ms`，按音频帧 100ms 递增；
- 画面时钟：`main_played_pts_ms`，由 `mpv-status` 事件里的播放位置增量累加；
- `sync_cue_base` 只在会话开始时校准一次，之后不再对齐；
- 播放器暂停、缓冲、mpv 位置上报跳变、直采时钟与播放位置漂移，都会让两者越差越多；
- 切换频道会重启会话，两个时钟归零，所以会“暂时正常”。

日志中 `start - render` 最大 +2.4 秒，正好落在用户感知的“慢一两秒”范围内。

## 8. 已排除 / 已验证无效的手段

| 手段 | 结果 |
|---|---|
| 调 `syncOffsetMs` 到 -6 秒 | 无效；文字没到，提前不了 |
| 默认 +1.5 秒 | 只是把窗口整体后移，delta 稳定在 +1500，但用户仍觉得慢 |
| 让未定稿句也上屏（出第一个字就换） | 中文会闪、会被改写，用户明确不接受 |
| 等整句中文定稿再换 | 中文不闪了，但整体慢 1～2 秒（当前状态） |
| 限制未定稿窗口到 6 秒 | 已修复“旧句 3～5 秒后重复上屏/跳回” |
| 定稿晚到重新锚到 `render_now` | 已修复“中文只闪一下” |
| 只显示定稿句 | 已实现；中文完整，但延迟是结构性的 |

## 9. 请 agy 重点分析的问题

### Q1. 如何把 A（模型定稿延迟）和 B（时钟误差）分开测量？

建议的最小实验：

- 在 `tick` 日志里同时打印 `decoded_audio_ms` 和 `main_played_pts_ms`，每秒一行，观察两者差值是否随时间增长；
- 用一段已知内容的测试视频，记录“人声开口时刻”和“字幕上屏时刻”；
- 对比 `response.text.done` 到达时的 `latest_audio` 与该句 `start`，算出模型从开口到定稿的真实耗时。

### Q2. 如果 A 是主因，显示策略应该怎么改？

在“中文必须完整”的前提下，可选：

- (a) 只显示“稳定前缀”：译文流式输出时，只显示已经不会被改写的部分，剩余部分到定稿再补；
- (b) 缩短分句：把 7 秒 / 70 字改成 2～3 秒或按逗号/从句切，让每句译文更早定稿；
- (c) 英文立即上屏 + 中文跟读：英文准，中文固定晚约 1 秒，但英文和中文短暂不同步；
- (d) 接受“一句延迟”：当前句播放时显示上一句译文，节奏稳定但整体晚一句。

请判断哪一种最符合“中文及时 + 完整 + 不闪”的组合，以及是否值得改模型/提示词。

### Q3. 如果 B 是主因，最小修复是什么？

候选：

- 每次 `tick` 或每隔 N 秒重新计算 `base = decoded_audio_now - main_played_now`，并平滑平移窗口；
- 播放器暂停时同时暂停采集时钟，或在恢复时重新 `sync_cue_base`；
- 直接用采集时钟作为显示时钟，不再维护 `main_played_pts_ms` 累加器；
- 修复 FFmpeg 探路器（`-extension_picky` 不被当前 FFmpeg 支持），让 standalone scout 路径可用，再比较两条路径的时序差异。

### Q4. `next_cue_start_after_release` 是否在制造假的句子起点？

当前实现：

```rust
let compensated = latest_audio_pts_ms.saturating_sub(REALTIME_TRANSLATION_LAG_MS);
self.last_released_start_pts_ms
    .map_or(compensated, |start| compensated.max(start.saturating_add(1)))
```

这会把下一句起点设为“上一句定稿时的采集时钟 - 1.5 秒”，而不是真实的 VAD 开口时刻。它可能让句子起点偏早或偏晚，进而影响排片。是否应该改为只用 VAD 起点？

### Q5. 分句参数是否应该缩短？

`REALTIME_CUE_MAX_DURATION_MS = 7000`、`REALTIME_CUE_MAX_TRANSLATED_CHARS = 70` 对“译文尽早定稿”不利。改成 3～4 秒 / 30～40 字，是否能显著降低 A？代价是句子更碎、翻译质量可能下降。

### Q6. FFmpeg `-extension_picky` 失败要不要先修？

当前 3 次探路全失败，实际一直走直采回退。直采路径的时钟问题可能正是 B 的来源。修好探路器后，standalone 路径有 5 秒预解码缓冲，时序特性可能完全不同。这是独立 bug，但可能影响主问题。

### Q7. 在“每次只改一处 + cargo check”的约束下，下一步最小改动是什么？

请给出一个明确的单点改动建议，并说明预期能从日志里看到什么变化（例如 `delta` 收敛到 0、`start - render` 不再增长、或译文定稿提前）。

## 10. 工程约束与现场信息

- 用户要求：只改最小范围，不重构；每次只改一处，改完立刻 `cargo check`；试 3 次修不好就停下来列报错和已试方法（目前 3 次已用完，进入分析阶段）。
- 构建/测试命令（PowerShell）：

```powershell
cd D:\ynotv-src\packages\app\src-tauri
$env:CARGO_HOME='D:\Rust\.cargo'
$env:RUSTUP_HOME='D:\Rust\.rustup'
$env:Path='D:\Rust\.cargo\bin;'+$env:Path
cargo check
cargo test --lib ai_subtitle
```

- 前端类型检查：

```powershell
cd D:\ynotv-src\packages\ui
npx tsc --noEmit -p tsconfig.json
```

- 日志：`C:\Users\Penn\AppData\Local\com.ynotv.app\logs\ynotv.log`（按 40KB 轮转，历史文件同目录）。
- 配置：`C:\Users\Penn\AppData\Roaming\com.ynotv.app\ynotv_ai_subtitle.json`。
- 相关文件：
  - `packages/app/src-tauri/src/ai_subtitle/controller.rs`
  - `packages/app/src-tauri/src/ai_subtitle/engine.rs`
  - `packages/app/src-tauri/src/ai_subtitle/models.rs`
  - `packages/app/src-tauri/src/ai_subtitle/audio_scout.rs`
  - `packages/app/src-tauri/src/ai_subtitle/config.rs`
  - `packages/ui/src/components/AiSubtitleOverlay.tsx`
  - `packages/ui/src/components/AiSubtitleOverlay.css`
  - `packages/ui/src/components/AiSubtitleSettingsModal.tsx`
  - `packages/ui/src/components/aiSubtitleTypes.ts`

## 11. 给用户的一句话

在 agy 给出方向前，建议先把「字幕延迟补偿」重置为 **0 秒**（当前是 -5 秒），否则负补偿会让字幕窗口提前结束，反而更容易闪。
