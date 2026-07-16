const express = require("express");
const router = express.Router();
const { db } = require("../services/firebase");
const mtproto = require("../services/mtproto");

function reqId() {
    return Math.random().toString(36).slice(2, 8);
}

function log(rid, level, msg, meta) {
    const line = `[stream:${rid}] ${msg}`;
    const payload = meta ? { ...meta } : undefined;
    if (level === "error") console.error(line, payload || "");
    else if (level === "warn") console.warn(line, payload || "");
    else console.log(line, payload || "");
}

router.get("/:id", async (req, res) => {
    const rid = reqId();
    const { id } = req.params;
    let aborted = false;

    // Track client disconnects to prevent memory leaks and zombie streams
    const onAbort = () => { aborted = true; };
    req.on("aborted", onAbort);
    res.on("close", onAbort);

    try {
        if (!id || typeof id !== "string") {
            return res.status(400).json({ error: "Invalid ID" });
        }

        const doc = await db.collection("videos").doc(id).get();
        if (!doc.exists) {
            return res.status(404).json({ error: "Video not found" });
        }

        const video = doc.data();
        if (!video.channelId || !video.messageId) {
            return res.status(404).json({ error: "Incomplete metadata" });
        }

        const src = await mtproto.resolveVideoSource(video.channelId, video.messageId);
        const totalSize = src.size;
        
        // Fix 5: Detect MIME type and fallback
        let mimeType = src.mimeType;
        const ext = (src.fileName || "").toLowerCase();

if (!mimeType || mimeType === "application/octet-stream") {
    if (ext.endsWith(".mkv")) {
        mimeType = "video/x-matroska";
    } else if (ext.endsWith(".webm")) {
        mimeType = "video/webm";
    } else if (ext.endsWith(".mov")) {
        mimeType = "video/quicktime";
    } else {
        mimeType = "video/mp4";
    }
}
        

        const rangeHeader = req.headers.range;
        let start = 0;
        let end = totalSize - 1;

        // Fix 1: Verify HTTP Response formatting for 206 Ranges
        if (rangeHeader) {
            const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader.trim());
            if (!match || (match[1] === "" && match[2] === "")) {
                return res.status(400).json({ error: "Malformed Range header" });
            }
            
            if (match[1] === "") {
                const suffix = parseInt(match[2], 10);
                start = Math.max(totalSize - suffix, 0);
            } else {
                start = parseInt(match[1], 10);
                end = match[2] === "" ? totalSize - 1 : parseInt(match[2], 10);
            }

            if (start > end || start >= totalSize) {
                res.set("Content-Range", `bytes */${totalSize}`);
                return res.status(416).json({ error: "Range not satisfiable" });
            }
            if (end >= totalSize) end = totalSize - 1;
        }

        const contentLength = end - start + 1;
        
        // Critical Streaming Headers (ETag + Cache-Control + Accept-Ranges)
        res.set({
            "Content-Type": mimeType,
            "Accept-Ranges": "bytes",
            "Content-Length": String(contentLength),
            "Cache-Control": "public, max-age=3600",
            "ETag": `"${id}-${totalSize}"`
        });

        if (rangeHeader) {
            res.status(206);
            res.set("Content-Range", `bytes ${start}-${end}/${totalSize}`);
        } else {
            res.status(200);
        }

        // Fix 8: Flush headers immediately so the browser initiates the video player UI
        res.flushHeaders();

        // Fix 7: Debug logging 
        log(rid, "info", "Stream initialized", { 
            videoId: id, 
            fileSize: totalSize, 
            mimeType, 
            requestedRange: rangeHeader || 'FULL', 
            startOffset: start, 
            endOffset: end 
        });

        // Delegate to MTProto to handle DC Migration and byte extraction
        await mtproto.streamRange(video.channelId, video.messageId, start, end, res, {
            onAbort: (handler) => {
                req.on("aborted", handler);
                res.on("close", handler);
            },
            videoId: id,
            mimeType: mimeType
        });

        // Fix 4 & 8: Verify stream and safely close the response
        if (!res.writableEnded && !aborted) {
            await new Promise((resolve) => res.end(resolve));
            log(rid, "info", "Final response status: Stream completed successfully", { videoId: id, totalBytesSent: contentLength });
        }

    } catch (err) {
        log(rid, "error", "Unhandled stream error", { videoId: id, error: err.message });
        if (!res.headersSent) {
            res.status(500).json({ error: "Stream failed" });
        } else if (!res.writableEnded) {
            // Fix 8: Destroy the socket instead of calling res.end() to prevent caching corrupted files
            res.destroy(err);
        }
    } finally {
        req.removeListener("aborted", onAbort);
        res.removeListener("close", onAbort);
    }
});

module.exports = router;
