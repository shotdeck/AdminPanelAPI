/*
 * The file browser shared by the customer page and the admin page.
 * Bts.FileBrowser({ root, api, base, onChange }) renders into `root` and talks
 * to `${base}/list`, `${base}/upload/start`, ... (see SpaceFilesControllerBase).
 * Files go straight to R2 with presigned URLs; the API never sees the bytes.
 */
(function () {
    "use strict";

    var el = Bts.el;
    var PART_BATCH = 20;
    var PART_CONCURRENCY = 4;
    var FILE_CONCURRENCY = 2;
    var MAX_TRIES = 4;

    function FileBrowser(opts) {
        this.root = opts.root;
        this.api = opts.api;
        this.base = opts.base;
        this.onChange = opts.onChange || function () {};
        this.customerLabel = opts.customerLabel || "You";
        this.zipTicketUrl = opts.zipTicketUrl || null;
        this.path = "";
        this.listing = null;
        this.selected = {};
        this.queue = [];
        this.active = 0;
        this.build();
    }

    FileBrowser.prototype.build = function () {
        var self = this;
        this.root.innerHTML = "";

        this.fileInput = el("input", { type: "file", multiple: true, class: "hidden" });
        this.fileInput.addEventListener("change", function () {
            self.enqueue(Array.prototype.slice.call(self.fileInput.files).map(function (f) {
                return { file: f, rel: "" };
            }));
            self.fileInput.value = "";
        });
        this.folderInput = el("input", { type: "file", multiple: true, webkitdirectory: true, class: "hidden" });
        this.folderInput.addEventListener("change", function () {
            self.enqueue(Array.prototype.slice.call(self.folderInput.files).map(function (f) {
                var rel = f.webkitRelativePath || f.name;
                return { file: f, rel: rel.slice(0, rel.length - f.name.length) };
            }));
            self.folderInput.value = "";
        });

        this.crumbs = el("div", { class: "crumbs" });
        this.bulkBar = el("div", { class: "row hidden" }, [
            el("span", { class: "muted", id: "bulk-count" }),
            this.zipTicketUrl ? el("button", { type: "button", text: "Download zip", onclick: function () { self.downloadZip(Object.keys(self.selected)); } }) : null,
            el("button", { type: "button", text: "Move to…", onclick: function () { self.moveSelected(); } }),
            el("button", { type: "button", class: "danger", text: "Delete", onclick: function () { self.deleteSelected(); } }),
            el("button", { type: "button", text: "Clear", onclick: function () { self.selected = {}; self.render(); } })
        ]);

        var toolbar = el("div", { class: "toolbar" }, [
            this.crumbs,
            this.bulkBar,
            this.zipTicketUrl ? el("button", { type: "button", text: "Download all", title: "Download everything in this folder as a zip", onclick: function () { self.downloadZip([]); } }) : null,
            el("button", { type: "button", text: "New folder", onclick: function () { self.newFolder(); } }),
            el("button", { type: "button", text: "Upload folder", onclick: function () { self.folderInput.click(); } }),
            el("button", { type: "button", class: "primary", text: "Upload files", onclick: function () { self.fileInput.click(); } }),
            this.fileInput, this.folderInput
        ]);

        this.drop = el("div", { class: "drop", text: "Drag photos and videos here to upload them into this folder." });
        this.status = el("div", { class: "muted" });
        this.grid = el("div", { class: "grid" });
        this.uploads = el("div", { class: "uploads" });

        this.root.appendChild(toolbar);
        this.root.appendChild(this.drop);
        this.root.appendChild(this.status);
        this.root.appendChild(this.grid);
        document.body.appendChild(this.uploads);

        ["dragenter", "dragover"].forEach(function (t) {
            self.root.addEventListener(t, function (e) {
                if (!e.dataTransfer || Array.prototype.indexOf.call(e.dataTransfer.types, "Files") < 0) return;
                e.preventDefault();
                self.drop.classList.add("over");
            });
        });
        ["dragleave", "drop"].forEach(function (t) {
            self.root.addEventListener(t, function () { self.drop.classList.remove("over"); });
        });
        this.root.addEventListener("drop", function (e) {
            if (!e.dataTransfer) return;
            e.preventDefault();
            collectDropped(e.dataTransfer).then(function (items) { self.enqueue(items); });
        });
    };

    FileBrowser.prototype.destroy = function () {
        this.uploads.remove();
        this.root.innerHTML = "";
    };

    FileBrowser.prototype.load = async function (path) {
        var target = path == null ? this.path : path;
        this.status.textContent = "Loading…";
        try {
            this.listing = await this.api.get(this.base + "/list?path=" + encodeURIComponent(target));
            this.path = this.listing.path;
            this.selected = {};
            this.status.textContent = "";
        } catch (e) {
            if (e.status === 404 && target !== "") return this.load("");
            this.status.textContent = e.message;
            return;
        }
        this.render();
    };

    FileBrowser.prototype.render = function () {
        var self = this;
        var listing = this.listing;
        if (!listing) return;

        this.crumbs.innerHTML = "";
        this.crumbs.appendChild(el("button", { type: "button", text: "All files", onclick: function () { self.load(""); } }));
        var parts = listing.path.split("/").filter(Boolean);
        parts.forEach(function (name, i) {
            var p = parts.slice(0, i + 1).join("/") + "/";
            self.crumbs.appendChild(el("span", { class: "sep", text: "/" }));
            self.crumbs.appendChild(el("button", { type: "button", text: name, onclick: function () { self.load(p); } }));
        });

        var count = Object.keys(this.selected).length;
        this.bulkBar.classList.toggle("hidden", count === 0);
        this.bulkBar.querySelector("#bulk-count").textContent = count + " selected";

        this.grid.innerHTML = "";
        listing.folders.forEach(function (f) { self.grid.appendChild(self.folderTile(f)); });
        listing.files.forEach(function (f) { self.grid.appendChild(self.fileTile(f)); });

        if (!listing.folders.length && !listing.files.length)
            this.status.textContent = listing.path ? "This folder is empty." : "Nothing here yet. Upload some photos or videos to get started.";
        else if (listing.truncated)
            this.status.textContent = "Showing the first few thousand items in this folder.";
        else
            this.status.textContent = "";
    };

    FileBrowser.prototype.checkbox = function (path) {
        var self = this;
        var box = el("input", { type: "checkbox", class: "check" });
        box.checked = !!this.selected[path];
        box.addEventListener("change", function () {
            if (box.checked) self.selected[path] = true; else delete self.selected[path];
            self.render();
        });
        return box;
    };

    FileBrowser.prototype.folderTile = function (f) {
        var self = this;
        return el("div", { class: "tile folder" + (this.selected[f.path] ? " selected" : "") }, [
            this.checkbox(f.path),
            el("div", { class: "thumb", onclick: function () { self.load(f.path); } }, [el("span", { class: "folder-icon" })]),
            el("div", { class: "meta clickable", onclick: function () { self.load(f.path); } }, [el("div", { class: "name", title: f.name, text: f.name })]),
            this.noteBlock(f),
            el("div", { class: "actions" }, [
                this.noteButton(f),
                el("button", { type: "button", text: "Rename", onclick: function () { self.rename(f.path, f.name); } }),
                el("button", { type: "button", text: "Move", onclick: function () { self.move([f.path]); } }),
                el("button", { type: "button", class: "danger", text: "Delete", onclick: function () { self.remove([f.path]); } })
            ])
        ]);
    };

    FileBrowser.prototype.fileTile = function (f) {
        var self = this;
        var thumb = el("div", { class: "thumb", onclick: function () { self.view(f); } });
        if (f.kind === "image" && f.url) {
            thumb.appendChild(el("img", { src: f.url, alt: f.name, loading: "lazy" }));
        } else if (f.kind === "video" && f.url) {
            var v = el("video", { src: f.url + "#t=0.5", preload: "metadata", muted: true, playsinline: true });
            v.muted = true;
            v.addEventListener("error", function () { thumb.innerHTML = ""; thumb.appendChild(el("span", { class: "icon", text: "VIDEO" })); });
            thumb.appendChild(v);
        } else {
            thumb.appendChild(el("span", { class: "icon", text: "FILE" }));
        }

        return el("div", { class: "tile" + (this.selected[f.path] ? " selected" : "") }, [
            this.checkbox(f.path),
            thumb,
            el("div", { class: "meta" }, [
                el("div", { class: "name", title: f.name, text: f.name }),
                el("div", { class: "sub", text: Bts.formatBytes(f.sizeBytes) + " · " + Bts.formatDate(f.lastModified) })
            ]),
            this.noteBlock(f),
            el("div", { class: "actions" }, [
                el("button", { type: "button", text: "Download", onclick: function () { self.download(f.path); } }),
                this.noteButton(f),
                el("button", { type: "button", text: "Rename", onclick: function () { self.rename(f.path, f.name); } }),
                el("button", { type: "button", text: "Move", onclick: function () { self.move([f.path]); } }),
                el("button", { type: "button", class: "danger", text: "Delete", onclick: function () { self.remove([f.path]); } })
            ])
        ]);
    };

    FileBrowser.prototype.noteBlock = function (f) {
        var self = this;
        if (!f.note) return null;
        var by = f.note.updatedBy === "customer" ? this.customerLabel : f.note.updatedBy;
        return el("div", { class: "note", title: "Edit note", onclick: function () { self.editNote(f); } }, [
            el("div", { class: "note-text", text: f.note.text }),
            el("div", { class: "note-by", text: by + " · " + Bts.formatDate(f.note.updatedAt) })
        ]);
    };

    FileBrowser.prototype.noteButton = function (f) {
        var self = this;
        return el("button", { type: "button", text: f.note ? "Edit note" : "Add note", onclick: function () { self.editNote(f); } });
    };

    FileBrowser.prototype.editNote = async function (f) {
        var self = this;
        var v = await Bts.promptDialog("Note on " + f.name,
            [{ name: "note", type: "textarea", label: "Note (leave empty to remove it)", value: f.note ? f.note.text : "",
               placeholder: "e.g. who shot it, scene, anything we should know", maxlength: 2000 }],
            "Save");
        if (!v) return;
        var text = v.note.trim();
        if (text === (f.note ? f.note.text : "")) return;
        await this.run(function () { return self.api.post(self.base + "/note", { path: f.path, note: text }); },
            text ? "Note saved." : "Note removed.");
    };

    FileBrowser.prototype.view = function (f) {
        var self = this;
        if (!f.url) return this.download(f.path);
        var media = f.kind === "video"
            ? el("video", { src: f.url, controls: true, autoplay: true, playsinline: true })
            : el("img", { src: f.url, alt: f.name });
        var overlay = el("div", { class: "viewer" }, [
            media,
            el("div", { class: "bar" }, [
                el("span", { text: f.name }),
                el("button", { type: "button", text: "Download", onclick: function () { self.download(f.path); } }),
                el("button", { type: "button", text: "Close", onclick: close })
            ])
        ]);
        function close() { overlay.remove(); document.removeEventListener("keydown", onKey); }
        function onKey(e) { if (e.key === "Escape") close(); }
        overlay.addEventListener("click", function (e) { if (e.target === overlay) close(); });
        document.addEventListener("keydown", onKey);
        document.body.appendChild(overlay);
    };

    // ── Actions ────────────────────────────────────────────────

    FileBrowser.prototype.run = async function (fn, success) {
        try {
            await fn();
            if (success) Bts.toast(success);
        } catch (e) {
            Bts.toast(e.message, true);
        }
        await this.load();
        this.onChange();
    };

    FileBrowser.prototype.newFolder = async function () {
        var self = this;
        var v = await Bts.promptDialog("New folder", [{ name: "name", label: "Folder name", placeholder: "e.g. Day 1" }], "Create");
        if (!v || !v.name.trim()) return;
        await this.run(function () { return self.api.post(self.base + "/folder", { parent: self.path, name: v.name.trim() }); });
    };

    FileBrowser.prototype.rename = async function (path, name) {
        var self = this;
        var v = await Bts.promptDialog("Rename", [{ name: "name", label: "New name", value: name, selectBase: true }], "Rename");
        if (!v || !v.name.trim() || v.name.trim() === name) return;
        await this.run(function () { return self.api.post(self.base + "/rename", { path: path, newName: v.name.trim() }); }, "Renamed.");
    };

    FileBrowser.prototype.move = async function (paths) {
        var self = this;
        var v = await Bts.promptDialog("Move " + (paths.length === 1 ? "item" : paths.length + " items"),
            [{ name: "to", label: "Destination folder (leave empty for the top folder)", value: this.path, placeholder: "e.g. Day 1/Stills" }],
            "Move");
        if (!v) return;
        var to = v.to.trim().replace(/^\/+/, "");
        await this.run(async function () {
            for (var i = 0; i < paths.length; i++)
                await self.api.post(self.base + "/move", { path: paths[i], toFolder: to });
        }, "Moved.");
    };

    FileBrowser.prototype.remove = async function (paths) {
        var self = this;
        var what = paths.length === 1 ? "\u201C" + paths[0].replace(/\/$/, "").split("/").pop() + "\u201D" : paths.length + " items";
        var hasFolder = paths.some(function (p) { return /\/$/.test(p); });
        var ok = await Bts.confirmDialog("Delete " + what + "?",
            hasFolder ? "Folders are deleted with everything inside them." : "This removes it from your space.", "Delete");
        if (!ok) return;
        await this.run(async function () {
            for (var i = 0; i < paths.length; i++)
                await self.api.post(self.base + "/delete", { path: paths[i] });
        }, "Deleted.");
    };

    FileBrowser.prototype.moveSelected = function () { return this.move(Object.keys(this.selected)); };
    FileBrowser.prototype.deleteSelected = function () { return this.remove(Object.keys(this.selected)); };

    FileBrowser.prototype.download = async function (path) {
        try {
            var res = await this.api.get(this.base + "/download-url?path=" + encodeURIComponent(path));
            var a = el("a", { href: res.url, rel: "noreferrer" });
            document.body.appendChild(a);
            a.click();
            a.remove();
        } catch (e) {
            Bts.toast(e.message, true);
        }
    };

    FileBrowser.prototype.downloadZip = function (paths) {
        return Bts.downloadZip(this.api, this.zipTicketUrl, this.path, paths);
    };

    // ── Uploads ────────────────────────────────────────────────

    FileBrowser.prototype.enqueue = function (items) {
        var self = this;
        var folder = this.path;
        // Skip OS clutter such as .DS_Store that comes along with dropped folders.
        items = items.filter(function (it) { return it.file.name.charAt(0) !== "." && it.file.name !== "Thumbs.db"; });
        items.forEach(function (it) {
            var row = el("div", { class: "item" }, [
                el("div", { class: "name", text: it.rel + it.file.name }),
                el("div", { class: "sub muted", text: "Waiting… " + Bts.formatBytes(it.file.size) }),
                el("div", { class: "bar" }, [el("div")])
            ]);
            self.uploads.appendChild(row);
            self.queue.push({ file: it.file, folder: folder + it.rel, row: row });
        });
        window.addEventListener("beforeunload", warnUnload);
        this.pump();
    };

    function warnUnload(e) { e.preventDefault(); e.returnValue = ""; }

    FileBrowser.prototype.pump = function () {
        var self = this;
        while (this.active < FILE_CONCURRENCY && this.queue.length) {
            var job = this.queue.shift();
            this.active++;
            this.upload(job).then(function () {
                self.active--;
                if (!self.queue.length && !self.active) {
                    window.removeEventListener("beforeunload", warnUnload);
                    self.load();
                    self.onChange();
                }
                self.pump();
            });
        }
    };

    FileBrowser.prototype.upload = async function (job) {
        var sub = job.row.querySelector(".sub");
        var bar = job.row.querySelector(".bar > div");
        function progress(done) {
            var pct = job.file.size ? Math.min(100, Math.round(done / job.file.size * 100)) : 100;
            bar.style.width = pct + "%";
            sub.textContent = pct + "% of " + Bts.formatBytes(job.file.size);
        }
        try {
            var start = await this.api.post(this.base + "/upload/start", {
                folder: job.folder, fileName: job.file.name, sizeBytes: job.file.size
            });
            if (start.mode === "single") {
                await putWithRetry(start.url, job.file, start.contentType, progress);
                await this.api.post(this.base + "/upload/confirm", { path: start.path });
            } else {
                await this.multipart(job.file, start, progress);
            }
            job.row.classList.add("done");
            sub.textContent = start.overwrites ? "Uploaded (replaced the old copy)" : "Uploaded";
            bar.style.width = "100%";
            setTimeout(function () { job.row.remove(); }, 4000);
        } catch (e) {
            job.row.classList.add("failed");
            sub.textContent = e.message || "Upload failed.";
            var close = el("button", { type: "button", class: "link", text: "Dismiss", onclick: function () { job.row.remove(); } });
            job.row.appendChild(close);
        }
    };

    FileBrowser.prototype.multipart = async function (file, start, progress) {
        var self = this;
        var size = start.partSizeBytes;
        var count = Math.max(1, Math.ceil(file.size / size));
        var loaded = new Array(count + 1).fill(0);
        var etags = [];
        var urls = {};
        var next = 1;

        function report() { progress(loaded.reduce(function (a, b) { return a + b; }, 0)); }

        async function urlFor(n) {
            if (!urls[n]) {
                var nums = [];
                for (var i = n; i < n + PART_BATCH && i <= count; i++) nums.push(i);
                var res = await self.api.post(self.base + "/upload/parts", { path: start.path, uploadId: start.uploadId, partNumbers: nums });
                res.parts.forEach(function (p) { urls[p.partNumber] = p.url; });
            }
            return urls[n];
        }

        async function worker() {
            while (next <= count) {
                var n = next++;
                var blob = file.slice((n - 1) * size, Math.min(file.size, n * size));
                var tries = 0;
                for (;;) {
                    try {
                        var etag = await put(await urlFor(n), blob, null, function (b) { loaded[n] = b; report(); });
                        if (!etag) throw new Error("Storage didn't return a part tag. Ask ShotDeck to check the bucket's CORS.");
                        etags.push({ partNumber: n, eTag: etag });
                        break;
                    } catch (e) {
                        if (++tries >= MAX_TRIES) throw e;
                        delete urls[n];
                        loaded[n] = 0;
                        await sleep(1000 * tries);
                    }
                }
            }
        }

        try {
            var workers = [];
            for (var w = 0; w < Math.min(PART_CONCURRENCY, count); w++) workers.push(worker());
            await Promise.all(workers);
            etags.sort(function (a, b) { return a.partNumber - b.partNumber; });
            await this.api.post(this.base + "/upload/complete", { path: start.path, uploadId: start.uploadId, parts: etags });
        } catch (e) {
            this.api.post(this.base + "/upload/abort", { path: start.path, uploadId: start.uploadId }).catch(function () {});
            throw e;
        }
    };

    async function putWithRetry(url, blob, contentType, progress) {
        for (var tries = 1; ; tries++) {
            try {
                return await put(url, blob, contentType, progress);
            } catch (e) {
                if (tries >= MAX_TRIES) throw e;
                await sleep(1000 * tries);
            }
        }
    }

    /** PUTs a blob to a presigned URL and resolves to its ETag. */
    function put(url, blob, contentType, progress) {
        return new Promise(function (resolve, reject) {
            var xhr = new XMLHttpRequest();
            xhr.open("PUT", url);
            if (contentType) xhr.setRequestHeader("Content-Type", contentType);
            xhr.upload.onprogress = function (e) { if (e.lengthComputable) progress(e.loaded); };
            xhr.onload = function () {
                if (xhr.status >= 200 && xhr.status < 300) {
                    progress(blob.size);
                    resolve(xhr.getResponseHeader("ETag"));
                } else {
                    reject(new Error("Upload failed (" + xhr.status + ")."));
                }
            };
            xhr.onerror = function () { reject(new Error("Upload interrupted. Check your connection.")); };
            xhr.send(blob);
        });
    }

    function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

    /** Flattens dropped files and folders into [{file, rel}] where rel is the subfolder path. */
    async function collectDropped(dt) {
        var entries = [];
        if (dt.items && dt.items.length && dt.items[0].webkitGetAsEntry) {
            for (var i = 0; i < dt.items.length; i++) {
                var entry = dt.items[i].webkitGetAsEntry();
                if (entry) entries.push(entry);
            }
        }
        if (!entries.length) {
            return Array.prototype.slice.call(dt.files).map(function (f) { return { file: f, rel: "" }; });
        }

        var out = [];
        async function walk(entry, rel) {
            if (entry.isFile) {
                var file = await new Promise(function (res, rej) { entry.file(res, rej); });
                out.push({ file: file, rel: rel });
            } else if (entry.isDirectory) {
                var reader = entry.createReader();
                var batch;
                do {
                    batch = await new Promise(function (res, rej) { reader.readEntries(res, rej); });
                    for (var j = 0; j < batch.length; j++) await walk(batch[j], rel + entry.name + "/");
                } while (batch.length);
            }
        }
        for (var k = 0; k < entries.length; k++) await walk(entries[k], "");
        return out;
    }

    Bts.FileBrowser = FileBrowser;
})();
