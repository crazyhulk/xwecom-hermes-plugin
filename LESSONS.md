# 把 OpenClaw（TS）插件移植到 Hermes（Python）的常见坑

这份文档基于 2026/06/26 把 `wecom-openclaw-plugin`（TS 官方插件）移植成 Hermes Python 插件 `xwecom` 时踩到的真实问题。后续让 AI 做同类移植时，先把这份文档丢给它，能避免重蹈覆辙。

## 0. 总原则

**移植 = 对照源插件的 schema 和契约逐行翻译，不是凭直觉重写。**

具体来说：
- 凡是涉及"对方系统"的数据结构（WebSocket frame、HTTP 响应、配置字段），**必须读源插件的 TS interface 定义**确定字段名与层级，不要靠"猜常见命名"。
- 凡是涉及宿主框架（Hermes）的 API（`MessageEvent`、`PlatformConfig`、`BasePlatformAdapter`、插件加载机制），**必须读宿主框架的源码或文档**，不要假设字段长什么样。
- 任何"看起来似乎应该是这样"的字段映射，都要在代码里加 assert / 日志验证一次。

---

## 1. 错位 1：把宿主框架的字段当成自己说了算

### 案例 1.1：`MessageEvent` 的多媒体字段

错的写法：
```python
event = MessageEvent(
    text=text,
    images=cached_images,        # ← MessageEvent 根本没有 images 字段
)
```

报错：
```
MessageEvent.__init__() got an unexpected keyword argument 'images'
```

正确做法：先读 `gateway/platforms/base.py` 里 `@dataclass class MessageEvent`，看它定义了什么字段。Hermes 的约定是：
```python
media_urls: List[str] = field(default_factory=list)   # 本地文件路径
media_types: List[str] = field(default_factory=list)  # 每个 url 对应的类型
```

### 案例 1.2：`PlatformConfig.extra` 字段名

错的写法（config.yaml）：
```yaml
xwecom:
  extra:
    botid: xxx       # adapter 里读的是 extra.get("bot_id")
    secret: yyy
```

报错（沉默）：`Platform 'XWeCom' config validation failed` —— adapter 永远拿不到 bot_id。

正确做法：移植时 **adapter 端读什么 key，config 模板就要给同名 key**。把 adapter 期望的 key 列在 README/plugin.yaml 里，让用户配置时有据可查。

### 案例 1.3：`MessageType` 枚举成员

错的写法：
```python
msg_type = MessageType.IMAGE   # ← Hermes 里没有这个成员
```

报错：`AttributeError: type object 'MessageType' has no attribute 'IMAGE'`，被外层捕获后只打 `xwecom: error - IMAGE`，看着像无意义字符串。

Hermes 的实际成员（`gateway/platforms/base.py`）：
```python
class MessageType(Enum):
    TEXT, LOCATION, PHOTO, VIDEO, AUDIO, VOICE, DOCUMENT, STICKER, COMMAND
```

注意：**图片是 `PHOTO`，不是 `IMAGE`**（命名跟 Telegram 的术语对齐，不跟 WeCom 的 `msgtype: "image"` 对齐）。

### 案例 1.4：`cache_image_from_bytes` 第二个参数是扩展名

错的写法：
```python
cache_image_from_bytes(img_data, "image.png")    # ← 把文件名当扩展名
```

正确：
```python
ext = os.path.splitext(filename)[1] or ".png"
cache_image_from_bytes(img_data, ext)
```

### 教训

- 写 `MessageEvent(...)`、`PlatformConfig(...)`、`SendResult(...)` 这些 dataclass 之前，先 `grep "@dataclass" gateway/platforms/base.py` 或直接打开看一眼字段列表。
- 任何 kwargs 不在字段列表里都会直接 `TypeError`，不要硬编。

---

## 2. 错位 2：把源插件的 frame schema 抄错层级

### 案例：把 `frame["body"]` 写成 `frame["data"]`

SDK 推送上来的 WebSocket frame 结构（`sdk/types.py`，对齐 TS）：
```python
WsFrame = {
    "cmd": str,
    "headers": {"req_id": str, ...},
    "body": {...},        # ← 真正的消息内容
    "errcode": int,
    "errmsg": str,
}
```

OpenClaw 的 `MessageBody` 字段（来自 `src/message-parser.ts`）：
```ts
{
  msgid: string,
  chatid?: string,
  chattype: "single" | "group",
  from: { userid: string, corpid?: string, chat_id?: string },
  msgtype: string,
  text?: { content: string },
  image?: { url, aeskey },
  ...
}
```

错的写法（症状：所有 user_id/chat_id 全是空字符串，消息进 gateway 后无 session 匹配，没有任何回复）：
```python
data = frame.get("data", {})           # 1. 错误层级
sender = data.get("sender", {})        # 2. 错误字段名（应该是 "from"）
user_id = sender.get("userid", "")
chat_id = data.get("chatid", "")
```

正确写法：
```python
body = frame.get("body") or {}
headers = frame.get("headers") or {}
msg_id = body.get("msgid") or headers.get("req_id") or ""
sender = body.get("from") or {}        # OpenClaw 用的是 "from" 不是 "sender"
user_id = sender.get("userid", "")
chat_id = body.get("chatid") or sender.get("chat_id") or ""
is_group = (body.get("chattype") or "").lower() == "group"
```

### 教训

- **移植任何"消息进入"流程时，先在源插件里 grep `body.from`、`body.chatid`、`body.chattype`，把字段路径列出来，再写 Python 解析代码。**
- 不要写 `_is_group_chat()` 这种"启发式判断"（按 chat_id 长度、是否含 `@` 等），TS 源插件直接读 `chattype` 字段就行，要忠实抄。
- 收消息的 entry 函数（`_on_message`）一开始就 `logger.debug("xwecom raw frame: %s", frame)`，跑一次就能验证 schema。

---

## 3. 错位 3：同步 vs 异步契约

### 案例：`await self._client.disconnect()`

SDK 里：
```python
def disconnect(self) -> None:  # 同步函数，返回 None
    ...
```

错的写法：
```python
async def disconnect(self):
    await self._client.disconnect()   # await None → TypeError
```

报错：
```
object NoneType can't be used in 'await' expression
```

### 教训

- 移植时不要凭直觉给所有 IO 加 `await`。**先看源插件该方法是不是 `async`**，TS 里 `async` 关键字很明显；Python SDK 里要 grep `async def` 才确认。
- 一个 SDK 内部，`connect()` 经常是 async（要握手、订阅），`disconnect()` 经常是 sync（只是关 socket 标志位）。不要假设两者一致。

---

## 4. 错位 4：Python ImportError 的"沉默吞错"

错的写法（来自最初版 `__init__.py`）：
```python
try:
    from .adapter import register
    __all__ = ["register"]
except ImportError:
    # Not running inside Hermes (e.g., pytest) — skip
    __all__ = []
```

后果：插件子模块里任何一个第三方依赖（如 `pyee`）没装，`from .adapter import register` 抛 `ModuleNotFoundError`（继承自 `ImportError`），被这个 try/except 默默吃掉。Hermes 加载完只打一行 WARNING：

```
Plugin 'xwecom-platform' has no register() function
```

完全看不出来真正缺什么、报什么。排查需要手动重放 import 链。

### 正确做法

**插件 `__init__.py` 不要 try/except 包裹 `from .adapter import register`。** 让异常直接冒到 Hermes，`_load_plugin` 会把 `str(exc)` 存到 `loaded.error` 并打 WARNING（`HERMES_PLUGINS_DEBUG=1` 时还带 traceback）。

如果真的要兼容"不在 Hermes 环境下也能 import"（比如跑 pytest），让 pytest 直接 import 具体模块而不是 import 包顶层：
```python
# 测试代码里
from xwecom.message_parser import parse_message_content  # 直连子模块，不走 __init__
```

### 教训

- 任何"捕获 ImportError 后默默 pass"都是反模式 —— 它把"代码 bug 导致的 ImportError"和"环境不具备的 ImportError"混为一谈。
- 如果真要捕获，至少 `logger.warning("…", exc_info=True)` 把堆栈打出来。

---

## 5. Hermes 插件加载的 gate 规则

User 插件（放在 `~/.hermes/profiles/<profile>/plugins/` 或 `~/.hermes/plugins/`）**必须** 显式 opt-in 才会被加载，因为 Hermes 把它们视为"未受信任的代码"。

### 启用方式

```bash
hermes plugins enable <plugin-name>
```

或者直接编辑 `config.yaml`：
```yaml
plugins:
  enabled:
    - <plugin-name>     # 用 plugin.yaml 里的 name
```

### 教训

- Bundled platform 插件（hermes 仓库 `plugins/platforms/` 下的）自动加载，但 user 插件不会 —— 移植自定义插件时 README 必须写明这一步。
- 排查"插件不生效"时第一步先 `hermes plugins list`，看 status 是 `enabled` 还是 `not enabled`。

---

## 6. 依赖管理

Hermes **不会** 自动 `pip install` 第三方插件 `requirements.txt` 的内容。装在 hermes 的 venv 里：

```bash
/Users/bilibili/.hermes/hermes-agent/venv/bin/python -m pip install -r requirements.txt
```

`register_platform(..., install_hint="pip install pyee websockets ...")` 这个参数只是给 `hermes plugins` 表格做展示用，**不会自动执行**。

---

## 7. 完整排查 checklist（按顺序检查）

每条都对应这次踩过的一个坑：

1. `hermes plugins list` —— 状态是 `enabled` 吗？
   - 不是 → `hermes plugins enable <name>`
2. 启 hermes 时有没有 `Plugin '...' has no register() function` 或 traceback？
   - 有 → 跑一次 `HERMES_PLUGINS_DEBUG=1 hermes plugins list`，或手动 import 插件包看真异常
   - 第三方依赖缺失 → `venv/bin/python -m pip install -r requirements.txt`
3. gateway.log 里 `Platform '...' config validation failed`？
   - adapter 的 `validate_config` 读的 key 和 config.yaml `extra:` 下的 key 不一致
4. 启 hermes 后 gateway 显示 `1 platform(s)` 但发消息没回复？
   - gateway.error.log 里大概率有 `MessageEvent.__init__() got an unexpected keyword argument '...'`
   - 或者类似 `... got an unexpected keyword argument` —— 看 hermes 端 dataclass 字段
5. 错误信息看起来都正常，但消息仍然没流到 agent？
   - 在 `_on_message` 入口加 `logger.info("xwecom raw frame: %s", frame)`，看实际 frame 的字段路径
   - 极大概率是 `frame["data"]` vs `frame["body"]`、`sender` vs `from` 这类 schema 抄错

---

## 8. 移植任务交给 AI 时建议在 prompt 里强调

1. **先列 schema 再写代码**：要求 AI 在动笔前先输出三张表：
   - 源插件（TS）的消息 body schema（字段路径 + 类型）
   - 源插件的关键回调签名（哪个是 async，哪个是 sync）
   - 宿主框架（Hermes）的 `MessageEvent` / `PlatformConfig` / `BasePlatformAdapter` 关键字段
   然后才开始写 adapter。
2. **不要写 try/except ImportError 包裹 `from .adapter import register`**，让异常往上抛。
3. **任何 `await xxx.disconnect/close/cleanup` 之前先确认它是 async**。
4. **`MessageEvent` 的多媒体字段是 `media_urls` + `media_types`，不是 `images`**。
5. **`chattype` 直接读 frame body 的字段**，不要写启发式判断 group vs dm。
6. **写完 adapter 在 `_on_message` 入口加一行 raw frame 日志**，至少在开发期保留，确认 schema 后再降级为 debug。
