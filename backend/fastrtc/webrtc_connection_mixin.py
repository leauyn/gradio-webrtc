"""Mixin for handling WebRTC connections."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from typing import (
    Any,
    Literal,
    ParamSpec,
    TypeVar,
    cast,
)

from aiortc import (
    RTCConfiguration,
    RTCIceCandidate,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.contrib.media import MediaRelay  # type: ignore
from anyio.to_thread import run_sync
from fastapi.responses import JSONResponse

from fastrtc.tracks import (
    AudioCallback,
    HandlerType,
    ServerToClientAudio,
    ServerToClientVideo,
    StreamHandlerBase,
    StreamHandlerFactory,
    StreamHandlerImpl,
    VideoCallback,
    VideoEventHandler,
    VideoStreamHandler,
    VideoStreamHandler_,
)
from fastrtc.utils import (
    AdditionalOutputs,
    Context,
    RTCConfigurationCallable,
    WebRTCData,
    create_message,
    webrtc_error_handler,
)

Track = (
    VideoCallback
    | VideoStreamHandler_
    | AudioCallback
    | ServerToClientAudio
    | ServerToClientVideo
)

logger = logging.getLogger(__name__)


# For the return type
R = TypeVar("R")
# For the parameter specification
P = ParamSpec("P")


@dataclass
class OutputQueue:
    queue: asyncio.Queue[AdditionalOutputs] = field(default_factory=asyncio.Queue)
    quit: asyncio.Event = field(default_factory=asyncio.Event)


class WebRTCConnectionMixin:
    def __init__(self):
        self.pcs: dict[str, RTCPeerConnection] = {}
        self.relay = MediaRelay()
        self.connections = defaultdict(list)
        self.data_channels = {}
        self.additional_outputs = defaultdict(OutputQueue)
        self.handlers: dict[str, HandlerType] = {}
        self.connection_timeouts = defaultdict(asyncio.Event)
        # These attributes should be set by subclasses:
        self.concurrency_limit: int | None
        self.event_handler: HandlerType | None
        self.time_limit: float | None
        self.modality: Literal["video", "audio", "audio-video"]
        self.mode: Literal["send", "receive", "send-receive"]
        self.allow_extra_tracks: bool
        self.rtc_configuration: dict[str, Any] | None | RTCConfigurationCallable | None
        self.server_rtc_configuration: RTCConfiguration | None

    @staticmethod
    async def wait_for_time_limit(pc: RTCPeerConnection, time_limit: float):
        await asyncio.sleep(time_limit)
        await pc.close()

    @staticmethod
    def convert_to_aiortc_format(
        rtc_configuration: dict[str, Any] | None,
    ) -> RTCConfiguration | None:
        rtc_config = rtc_configuration
        if rtc_config is not None:
            rtc_config = RTCConfiguration(
                iceServers=[
                    RTCIceServer(
                        urls=server["urls"],
                        username=server.get("username"),
                        credential=server.get("credential"),
                    )
                    for server in rtc_config.get("iceServers", [])
                ]
            )
        return rtc_config

    async def connection_timeout(
        self,
        pc: RTCPeerConnection,
        webrtc_id: str,
        time_limit: float,
    ):
        try:
            await asyncio.wait_for(
                self.connection_timeouts[webrtc_id].wait(), time_limit
            )
        except (asyncio.TimeoutError, TimeoutError):
            await pc.close()
            self.connection_timeouts[webrtc_id].clear()
            self.clean_up(webrtc_id)

    def clean_up(self, webrtc_id: str):
        self.handlers.pop(webrtc_id, None)
        self.connection_timeouts.pop(webrtc_id, None)
        self.pcs.pop(webrtc_id, None)
        connection = self.connections.pop(webrtc_id, [])
        for conn in connection:
            if isinstance(conn, AudioCallback):
                if inspect.iscoroutinefunction(conn.event_handler.shutdown):
                    asyncio.create_task(conn.event_handler.shutdown())
                    conn.event_handler.reset()
                else:
                    conn.event_handler.shutdown()
                    conn.event_handler.reset()
        output = self.additional_outputs.pop(webrtc_id, None)
        if output:
            logger.debug("setting quit for webrtc id %s", webrtc_id)
            output.quit.set()
        self.data_channels.pop(webrtc_id, None)
        return connection

    def set_input(self, webrtc_id: str, *args):
        if webrtc_id in self.connections:
            for conn in self.connections[webrtc_id]:
                conn.set_args(list(args))

    def set_input_gradio(self, webrtc_data: WebRTCData | str, *args):
        webrtc_id = webrtc_data
        if isinstance(webrtc_data, WebRTCData):
            webrtc_id = webrtc_data.webrtc_id
        self.set_input(cast(str, webrtc_id), webrtc_data, *args)

    def set_input_on_submit(self, webrtc_data: WebRTCData | str, *args):
        webrtc_id = webrtc_data
        if isinstance(webrtc_data, WebRTCData):
            webrtc_id = webrtc_data.webrtc_id
        self.set_input(cast(str, webrtc_id), webrtc_data, *args)
        if hasattr(self.handlers[cast(str, webrtc_id)], "trigger_response"):
            self.handlers[cast(str, webrtc_id)].trigger_response()  # type: ignore

    async def output_stream(
        self, webrtc_id: str
    ) -> AsyncGenerator[AdditionalOutputs, None]:
        outputs = self.additional_outputs[webrtc_id]
        while not outputs.quit.is_set():
            try:
                yield await asyncio.wait_for(outputs.queue.get(), 0.1)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Timeout waiting for output")

    async def fetch_latest_output(self, webrtc_id: str) -> AdditionalOutputs:
        outputs = self.additional_outputs[webrtc_id]
        return await asyncio.wait_for(outputs.queue.get(), 10)

    def set_additional_outputs(
        self, webrtc_id: str
    ) -> Callable[[AdditionalOutputs], None]:
        def set_outputs(outputs: AdditionalOutputs):
            self.additional_outputs[webrtc_id].queue.put_nowait(outputs)

        return set_outputs

    async def resolve_rtc_configuration(self) -> dict[str, Any] | None:
        if inspect.isfunction(self.rtc_configuration):
            if inspect.iscoroutinefunction(self.rtc_configuration):
                return await self.rtc_configuration()
            else:
                return await run_sync(self.rtc_configuration)
        else:
            return cast(dict[str, Any], self.rtc_configuration) or {}

    async def _trigger_response(self, webrtc_id: str, args: list[Any] | None = None):
        from fastrtc import ReplyOnPause

        if webrtc_id in self.connections and isinstance(
            self.handlers[webrtc_id], ReplyOnPause
        ):
            if args:
                cast(ReplyOnPause, self.handlers[webrtc_id]).set_args(args)
            cast(ReplyOnPause, self.handlers[webrtc_id]).trigger_response()
            return {"status": "success"}
        else:
            return {"status": "failed", "meta": {"error": "not_a_reply_on_pause"}}

    async def handle_offer(self, body, set_outputs):
        logger.debug("Starting to handle offer")
        logger.debug("Offer body %s", body)
        
        # 详细排查请求体内容
        logger.info(f"🔍 WebRTC handle_offer 请求体排查:")
        logger.info(f"  - body类型: {type(body)}")
        logger.info(f"  - body内容: {body}")
        logger.info(f"  - body中的userId: {body.get('userId', 'NOT_FOUND')}")
        logger.info(f"  - body中的webrtc_id: {body.get('webrtc_id', 'NOT_FOUND')}")
        logger.info(f"  - body中的type: {body.get('type', 'NOT_FOUND')}")

        if body.get("type") == "ice-candidate" and "candidate" in body:
            webrtc_id = body.get("webrtc_id")
            if webrtc_id not in self.pcs:
                logger.warning(
                    f"Received ICE candidate for unknown connection: {webrtc_id}"
                )
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "failed",
                        "meta": {"error": "unknown_connection"},
                    },
                )

            pc = self.pcs[webrtc_id]
            if pc.connectionState != "closed":
                try:
                    candidate_str = body["candidate"].get("candidate", "")

                    # Example format: "candidate:2393089663 1 udp 2122260223 192.168.86.60 63692 typ host generation 0 ufrag LkZb network-id 1 network-cost 10"
                    parts = candidate_str.split()
                    if len(parts) >= 10 and parts[0].startswith("candidate:"):
                        foundation = parts[0].split(":", 1)[1]
                        component = int(parts[1])
                        protocol = parts[2]
                        priority = int(parts[3])
                        ip = parts[4]
                        port = int(parts[5])
                        # Find the candidate type
                        typ_index = parts.index("typ")
                        candidate_type = parts[typ_index + 1]

                        # Create the RTCIceCandidate object
                        ice_candidate = RTCIceCandidate(
                            component=component,
                            foundation=foundation,
                            ip=ip,
                            port=port,
                            priority=priority,
                            protocol=protocol,
                            type=candidate_type,
                            sdpMid=body["candidate"].get("sdpMid"),
                            sdpMLineIndex=body["candidate"].get("sdpMLineIndex"),
                        )

                        # ✅ 修复：在添加 ICE candidate 前检查 remote description 是否已设置
                        try:
                            await pc.addIceCandidate(ice_candidate)
                            logger.debug(f"Added ICE candidate for {webrtc_id}")
                        except AttributeError as e:
                            # Remote description 尚未设置，这是正常的（ICE candidate 可能在 offer 之前到达）
                            if "'NoneType' object has no attribute 'media'" in str(e):
                                logger.debug(f"ICE candidate received before remote description set for {webrtc_id}, will be added later")
                            else:
                                logger.warning(f"Error adding ICE candidate for {webrtc_id}: {e}")
                        except Exception as e:
                            logger.warning(f"Error adding ICE candidate for {webrtc_id}: {e}")
                        return JSONResponse(
                            status_code=200, content={"status": "success"}
                        )
                    else:
                        logger.error(f"Invalid candidate format: {candidate_str}")
                        return JSONResponse(
                            status_code=200,
                            content={
                                "status": "failed",
                                "meta": {"error": "invalid_candidate_format"},
                            },
                        )
                except Exception as e:
                    logger.error(f"Error adding ICE candidate: {e}", exc_info=True)
                    return JSONResponse(
                        status_code=200,
                        content={"status": "failed", "meta": {"error": str(e)}},
                    )

            return JSONResponse(
                status_code=200,
                content={"status": "failed", "meta": {"error": "connection_closed"}},
            )

        if body["webrtc_id"] in self.connections:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "failed",
                    "meta": {
                        "error": "connection_already_exists",
                    },
                },
            )

        if len(self.pcs) >= cast(int, self.concurrency_limit):
            return JSONResponse(
                status_code=200,
                content={
                    "status": "failed",
                    "meta": {
                        "error": "concurrency_limit_reached",
                        "limit": self.concurrency_limit,
                    },
                },
            )

        offer = RTCSessionDescription(sdp=body["sdp"], type=body["type"])

        pc = RTCPeerConnection(configuration=self.server_rtc_configuration)
        self.pcs[body["webrtc_id"]] = pc
        
        # ✅ 关键修复：在创建 PC 后立即注册 track 事件监听器，防止 track 事件在注册前触发
        @pc.on("track")
        def _(track):
            logger.info(f"[WEBRTC] 📡 track 事件触发: kind={track.kind}, id={track.id}, readyState={track.readyState}, modality={self.modality}")
            # 从 self.handlers 获取 handler，如果还没有设置则记录警告
            if body["webrtc_id"] not in self.handlers:
                logger.warning(f"[WEBRTC] ⚠️ track 事件触发时 handler 尚未设置，webrtc_id={body['webrtc_id']}")
                return
            
            relay = MediaRelay()
            handler = self.handlers[body["webrtc_id"]]
            context = Context(webrtc_id=body["webrtc_id"])
            if self.modality == "video" and track.kind == "video":
                args = {}
                handler_ = handler
                if isinstance(handler, VideoStreamHandler):
                    handler_ = handler.callable
                    args["fps"] = handler.fps
                    args["skip_frames"] = handler.skip_frames
                cb = VideoCallback(
                    relay.subscribe(track),
                    event_handler=cast(Callable, handler_),
                    set_additional_outputs=set_outputs,
                    mode=cast(Literal["send", "send-receive"], self.mode),
                    context=context,
                    **args,
                )
            elif self.modality == "audio-video" and track.kind == "video":
                cb = VideoStreamHandler_(
                    relay.subscribe(track),
                    event_handler=handler,  # type: ignore
                    set_additional_outputs=set_outputs,
                    fps=cast(StreamHandlerImpl, handler).fps,
                    context=context,
                )
            elif self.modality in ["audio", "audio-video"] and track.kind == "audio":
                # ✅ 修复：RemoteStreamTrack 没有 enabled 属性，使用 getattr 安全访问
                track_enabled = getattr(track, 'enabled', 'N/A')
                track_label = getattr(track, 'label', 'N/A')
                logger.info(f"[WEBRTC] 🎤 收到音频 track: id={track.id}, enabled={track_enabled}, readyState={track.readyState}, label={track_label}")
                eh = cast(StreamHandlerImpl, handler)
                eh._loop = asyncio.get_running_loop()
                subscribed_track = relay.subscribe(track)
                logger.info(f"[WEBRTC] ✅ 创建 AudioCallback: subscribed_track={subscribed_track.id if subscribed_track else 'None'}")
                cb = AudioCallback(
                    subscribed_track,
                    event_handler=eh,
                    set_additional_outputs=set_outputs,
                    context=context,
                )
                logger.info(f"[WEBRTC] ✅ AudioCallback 创建完成")
            else:
                if self.modality not in ["video", "audio", "audio-video"]:
                    msg = "Modality must be either video, audio, or audio-video"
                else:
                    if self.allow_extra_tracks:
                        return
                    msg = f"Unsupported track kind '{track.kind}' for modality '{self.modality}'"
                raise ValueError(msg)
            if body["webrtc_id"] not in self.connections:
                self.connections[body["webrtc_id"]] = []

            self.connections[body["webrtc_id"]].append(cb)
            if body["webrtc_id"] in self.data_channels:
                for conn in self.connections[body["webrtc_id"]]:
                    conn.set_channel(self.data_channels[body["webrtc_id"]])
            if self.mode == "send-receive":
                logger.debug("Adding track to peer connection %s", cb)
                pc.addTrack(cb)
            elif self.mode == "send":
                asyncio.create_task(cast(AudioCallback | VideoCallback, cb).start())

        if isinstance(self.event_handler, StreamHandlerBase):
            handler = self.event_handler.copy(webrtc_id=body['webrtc_id'])
            handler.emit = webrtc_error_handler(handler.emit)  # type: ignore
            handler.receive = webrtc_error_handler(handler.receive)  # type: ignore
            handler.start_up = webrtc_error_handler(handler.start_up)  # type: ignore
            handler.shutdown = webrtc_error_handler(handler.shutdown)  # type: ignore
            if hasattr(handler, "video_receive"):
                handler.video_receive = webrtc_error_handler(handler.video_receive)  # type: ignore
            if hasattr(handler, "video_emit"):
                handler.video_emit = webrtc_error_handler(handler.video_emit)  # type: ignore
            if hasattr(handler, "on_pc_connected"):
                handler.on_pc_connected(body["webrtc_id"])
        elif isinstance(self.event_handler, VideoStreamHandler):
            self.event_handler.callable = cast(
                VideoEventHandler, webrtc_error_handler(self.event_handler.callable)
            )
            handler = self.event_handler
        else:
            handler = webrtc_error_handler(cast(Callable, self.event_handler))

        # 设置用户ID到处理器
        # logger.info(f"🔍 WebRTC 用户ID设置排查开始:")
        # logger.info(f"  - body中的userId: {body.get('userId', 'NOT_FOUND')}")
        # logger.info(f"  - hasattr(handler, 'user_id'): {hasattr(handler, 'user_id')}")
        # logger.info(f"  - 'userId' in body: {'userId' in body}")
        
        # 尝试从body获取用户ID
        user_id = body.get('userId')
        
        # 如果body中没有用户ID，尝试从存储中获取
        if not user_id and 'webrtc_id' in body:
            try:
                import sys
                import os
                # 添加项目根目录到Python路径
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../..'))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                
                from src.utils.user_id_storage import get_user_id
                user_id = get_user_id(body['webrtc_id'])
                if user_id:
                    # logger.info(f"✅ 从存储中获取到用户ID: {user_id}")
                    pass
                else:
                    logger.warning(f"⚠️ 存储中未找到用户ID: webrtc_id={body['webrtc_id']}")
            except Exception as e:
                logger.error(f"⚠️ 从存储获取用户ID失败: {e}")
        
        if hasattr(handler, 'user_id') and user_id:
            old_user_id = getattr(handler, 'user_id', None)
            handler.user_id = user_id
            # logger.info(f"✅ 设置处理器用户ID: {old_user_id} -> {handler.user_id}")
            
            # 如果处理器有webrtc_id属性，也设置它
            if hasattr(handler, 'webrtc_id') and 'webrtc_id' in body:
                handler.webrtc_id = body['webrtc_id']
                # logger.info(f"✅ 设置处理器WebRTC ID: {handler.webrtc_id}")
        else:
            logger.warning(f"⚠️ 无法设置处理器用户ID: hasattr={hasattr(handler, 'user_id')}, user_id={user_id}")

        self.handlers[body["webrtc_id"]] = handler

        @pc.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            logger.debug("ICE connection state change %s", pc.iceConnectionState)
            if pc.iceConnectionState == "failed":
                await pc.close()
                self.connections.pop(body["webrtc_id"], None)
                self.pcs.pop(body["webrtc_id"], None)

        @pc.on("connectionstatechange")
        async def _():
            logger.debug("pc.connectionState %s", pc.connectionState)
            if pc.connectionState in ["failed", "closed"]:
                await pc.close()
                connection = self.clean_up(body["webrtc_id"])
                if connection:
                    for conn in connection:
                        conn.stop()
                self.pcs.pop(body["webrtc_id"], None)
            if pc.connectionState == "connected":
                self.connection_timeouts[body["webrtc_id"]].set()
                if self.time_limit is not None:
                    asyncio.create_task(self.wait_for_time_limit(pc, self.time_limit))

        context = Context(webrtc_id=body["webrtc_id"])
        if self.mode == "receive":
            if self.modality == "video":
                if isinstance(self.event_handler, VideoStreamHandler):
                    cb = ServerToClientVideo(
                        cast(Callable, self.event_handler.callable),
                        set_additional_outputs=set_outputs,
                        fps=self.event_handler.fps,
                        context=context,
                    )
                else:
                    cb = ServerToClientVideo(
                        cast(Callable, self.event_handler),
                        set_additional_outputs=set_outputs,
                        context=context,
                    )
            elif self.modality == "audio":
                cb = ServerToClientAudio(
                    cast(Callable, self.event_handler),
                    set_additional_outputs=set_outputs,
                    context=context,
                )
            else:
                raise ValueError("Modality must be either video or audio")

            logger.debug("Adding track to peer connection %s", cb)
            pc.addTrack(cb)
            self.connections[body["webrtc_id"]].append(cb)
            cb.on("ended", lambda: self.clean_up(body["webrtc_id"]))

        @pc.on("datachannel")
        def _(channel):
            logger.debug(f"Data channel established: {channel.label}")

            self.data_channels[body["webrtc_id"]] = channel

            async def set_channel(webrtc_id: str):
                while not self.connections.get(webrtc_id):
                    await asyncio.sleep(0.05)
                logger.debug("setting channel for webrtc id %s", webrtc_id)
                for conn in self.connections[webrtc_id]:
                    conn.set_channel(channel)

            asyncio.create_task(set_channel(body["webrtc_id"]))

            @channel.on("message")
            def _(message):
                logger.debug(f"Received message: {message}")
                if channel.readyState == "open":
                  def parse_json_safely(str: str):
                    try:
                        result = json.loads(str)
                        return result, None
                    except json.JSONDecodeError as e:
                        # print(f"JSON解析错误: {e.msg}")
                        return None, e
                  msg_dict,error = parse_json_safely(message)
                  if(error is None and msg_dict['type'] in ['chat','stop_chat', 'init']):
                    msg_dict = cast(dict, json.loads(message))
                    handler = self.handlers[body["webrtc_id"]]
                    if inspect.iscoroutinefunction(handler.on_chat_datachannel):
                        asyncio.create_task(
                            handler.on_chat_datachannel(msg_dict,channel))
                    else: 
                        handler.on_chat_datachannel(msg_dict,channel)
                  else:
                    channel.send(
                        create_message("log", data=f"Server received: {message}")
                    )

        # handle offer
        await pc.setRemoteDescription(offer)
        asyncio.create_task(self.connection_timeout(pc, body["webrtc_id"], 30))
        # send answer
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)  # type: ignore
        logger.debug("done handling offer about to return")
        await asyncio.sleep(0.1)

        return {
            "sdp": pc.localDescription.sdp,
            "type": pc.localDescription.type,
        }
