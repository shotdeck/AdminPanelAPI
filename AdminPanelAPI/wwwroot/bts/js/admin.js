/* Admin page: sign in, manage customer spaces and their links, browse any space. */
(function () {
    "use strict";

    var el = Bts.el;
    var STORE = "bts.admin";
    var session = null;
    var spaces = [];
    var current = null;
    var browser = null;

    try { session = JSON.parse(sessionStorage.getItem(STORE) || "null"); } catch (e) { session = null; }
    if (session && new Date(session.expiresAt) <= new Date()) session = null;

    var api = Bts.makeApi(function () { return session && session.token; }, function () { signOut(); });

    function $(id) { return document.getElementById(id); }

    function show(section) {
        ["login", "spaces", "space"].forEach(function (s) { $(s).classList.toggle("hidden", s !== section); });
        $("logout").classList.toggle("hidden", !session);
        $("who").textContent = session ? session.name : "";
    }

    function signOut() {
        session = null;
        sessionStorage.removeItem(STORE);
        closeSpace();
        show("login");
    }

    $("login").addEventListener("submit", async function (e) {
        e.preventDefault();
        $("login-error").textContent = "";
        try {
            var res = await api.post("/api/bts/admin/login", { name: $("login-name").value, password: $("login-password").value });
            session = res;
            sessionStorage.setItem(STORE, JSON.stringify(res));
            $("login-password").value = "";
            route();
        } catch (err) {
            $("login-error").textContent = err.message;
        }
    });
    $("logout").addEventListener("click", signOut);

    // ── Space list ─────────────────────────────────────────────

    async function loadSpaces() {
        show("spaces");
        try {
            spaces = await api.get("/api/bts/admin/spaces");
        } catch (e) {
            Bts.toast(e.message, true);
            spaces = [];
        }
        renderSpaces();
    }

    function statusBadge(s) {
        if (s.revokedAt) return el("span", { class: "badge off", text: "Link off" });
        if (!s.active) return el("span", { class: "badge off", text: "Expired" });
        return el("span", { class: "badge ok", text: "Active" });
    }

    function renderSpaces() {
        var q = $("filter").value.trim().toLowerCase();
        var rows = $("space-rows");
        rows.innerHTML = "";
        spaces.filter(function (s) {
            return !q || s.name.toLowerCase().indexOf(q) >= 0 || (s.notes || "").toLowerCase().indexOf(q) >= 0;
        }).forEach(function (s) {
            rows.appendChild(el("tr", { class: "clickable", onclick: function () { location.hash = "#space/" + s.id; } }, [
                el("td", { class: "name-cell" }, [
                    el("div", { text: s.name }),
                    s.notes ? el("div", { class: "muted", title: s.notes, text: s.notes }) : null
                ]),
                el("td", null, [statusBadge(s)]),
                el("td", { class: "hide-sm", text: s.expiresAt ? Bts.formatDate(s.expiresAt) : "Never" }),
                el("td", { class: "hide-sm muted", text: Bts.formatDate(s.createdAt) + " · " + s.createdBy }),
                el("td", { class: "actions-cell" }, [el("button", { type: "button", class: "small ghost row-action", text: "Download all",
                    title: "Download everything in this space as a zip",
                    onclick: function (e) { e.stopPropagation(); zipSpace(s.id); } })])
            ]));
        });
        $("spaces-empty").classList.toggle("hidden", spaces.length > 0);
    }

    $("filter").addEventListener("input", renderSpaces);

    function spaceFields(s) {
        return [
            { name: "name", label: "Customer / space name", value: s ? s.name : "", placeholder: "e.g. Acme Films – Night Shoot" },
            { name: "notes", label: "Notes (only admins see these)", type: "textarea", value: s ? s.notes : "" },
            { name: "expires", label: "Link expires (optional)", type: "date", value: s && s.expiresAt ? s.expiresAt.slice(0, 10) : "" },
            { name: "quota", label: "Storage limit in GB (optional)", type: "number", step: "any",
              value: s && s.quotaBytes ? +(s.quotaBytes / Math.pow(1024, 3)).toFixed(2) : "" }
        ];
    }

    function spaceBody(v) {
        var gb = parseFloat(v.quota);
        return {
            name: v.name.trim(),
            notes: v.notes.trim() || null,
            // The link stops working at the end of the chosen day, local time.
            expiresAt: v.expires ? new Date(v.expires + "T23:59:59").toISOString() : null,
            quotaBytes: gb > 0 ? Math.round(gb * Math.pow(1024, 3)) : null
        };
    }

    $("new-space").addEventListener("click", async function () {
        var v = await Bts.promptDialog("New customer space", spaceFields(null), "Create");
        if (!v || !v.name.trim()) return;
        try {
            var s = await api.post("/api/bts/admin/spaces", spaceBody(v));
            location.hash = "#space/" + s.id;
            Bts.toast("Space created. Add folders or files, then use Share link to send it to the customer.");
        } catch (e) {
            Bts.toast(e.message, true);
        }
    });

    // ── One space ──────────────────────────────────────────────

    function closeSpace() {
        if (browser) { browser.destroy(); browser = null; }
        setShare(false);
        current = null;
    }

    async function openSpace(id) {
        closeSpace();
        show("space");
        try {
            current = await api.get("/api/bts/admin/spaces/" + id);
        } catch (e) {
            Bts.toast(e.message, true);
            location.hash = "";
            return;
        }
        renderSpace();
        browser = new Bts.FileBrowser({
            root: $("tab-files"),
            api: api,
            base: "/api/bts/admin/spaces/" + id + "/files",
            customerLabel: "Customer",
            zipTicketUrl: "/api/bts/admin/spaces/" + id + "/zip-ticket",
            onChange: refreshUsage
        });
        selectTab("files");
        browser.load("");
        refreshUsage();
    }

    function renderSpace() {
        var s = current;
        $("space-title").textContent = s.name;
        var badge = statusBadge(s);
        $("space-status").className = badge.className;
        $("space-status").textContent = badge.textContent;
        $("space-meta").textContent = "Created " + Bts.formatDate(s.createdAt) + " by " + s.createdBy +
            " · " + (s.expiresAt ? "Link expires " + Bts.formatDate(s.expiresAt) : "Link never expires") +
            (s.quotaBytes ? " · Limit " + Bts.formatBytes(s.quotaBytes) : "");
        $("space-notes").textContent = s.notes || "";
        $("space-notes").classList.toggle("hidden", !s.notes);
        $("space-link").value = s.active ? (s.link || "") : "Link is off. Create a new link to turn it back on.";
        $("copy-link").disabled = !s.active || !s.link;
        $("share-link").disabled = !s.active || !s.link;
        if ($("share-link").disabled) setShare(false);
        $("revoke-link").disabled = !!s.revokedAt;
    }

    async function refreshUsage() {
        if (!current) return;
        try {
            var u = await api.get("/api/bts/admin/spaces/" + current.id + "/usage");
            $("space-usage").textContent = u.files + " files · " + Bts.formatBytes(u.bytes) +
                (u.quotaBytes ? " of " + Bts.formatBytes(u.quotaBytes) : "");
        } catch (e) { /* informational */ }
    }

    $("back").addEventListener("click", function () { location.hash = ""; });

    function setShare(open) {
        $("share-box").classList.toggle("hidden", !open);
        $("share-link").textContent = open ? "Hide link" : "Share link";
    }

    $("share-link").addEventListener("click", async function () {
        var open = $("share-box").classList.contains("hidden");
        setShare(open);
        if (!open) return;
        try {
            await navigator.clipboard.writeText($("space-link").value);
            Bts.toast("Link copied. Send it to the customer.");
        } catch (e) {
            $("space-link").select();
        }
    });

    $("copy-link").addEventListener("click", async function () {
        try {
            await navigator.clipboard.writeText($("space-link").value);
            Bts.toast("Link copied.");
        } catch (e) {
            $("space-link").select();
            Bts.toast("Press Ctrl+C to copy.");
        }
    });

    function zipSpace(id) {
        return Bts.downloadZip(api, "/api/bts/admin/spaces/" + id + "/zip-ticket", "", []);
    }

    $("zip-space").addEventListener("click", function () { zipSpace(current.id); });

    $("edit-space").addEventListener("click", async function () {
        var v = await Bts.promptDialog("Edit space", spaceFields(current), "Save");
        if (!v || !v.name.trim()) return;
        try {
            current = await api.put("/api/bts/admin/spaces/" + current.id, spaceBody(v));
            renderSpace();
        } catch (e) { Bts.toast(e.message, true); }
    });

    $("rotate-link").addEventListener("click", async function () {
        var ok = await Bts.confirmDialog("Create a new link?",
            "The current link stops working straight away. Send the customer the new one.", "New link");
        if (!ok) return;
        try {
            current = await api.post("/api/bts/admin/spaces/" + current.id + "/rotate-link");
            renderSpace();
            Bts.toast("New link created.");
        } catch (e) { Bts.toast(e.message, true); }
    });

    $("revoke-link").addEventListener("click", async function () {
        var ok = await Bts.confirmDialog("Turn off this link?",
            "The customer won't be able to open the space. Files are kept. You can create a new link later.", "Turn off");
        if (!ok) return;
        try {
            current = await api.post("/api/bts/admin/spaces/" + current.id + "/revoke");
            renderSpace();
        } catch (e) { Bts.toast(e.message, true); }
    });

    // ── Tabs ───────────────────────────────────────────────────

    function selectTab(name) {
        Array.prototype.forEach.call(document.querySelectorAll(".tabs button"), function (b) {
            b.classList.toggle("active", b.getAttribute("data-tab") === name);
        });
        ["files", "trash", "activity"].forEach(function (t) { $("tab-" + t).classList.toggle("hidden", t !== name); });
        if (name === "trash") loadTrash();
        if (name === "activity") loadActivity();
        if (name === "files" && browser) browser.load();
    }

    Array.prototype.forEach.call(document.querySelectorAll(".tabs button"), function (b) {
        b.addEventListener("click", function () { selectTab(b.getAttribute("data-tab")); });
    });

    async function loadTrash() {
        var box = $("tab-trash");
        box.textContent = "Loading…";
        var items;
        try {
            items = await api.get("/api/bts/admin/spaces/" + current.id + "/trash");
        } catch (e) { box.textContent = e.message; return; }
        box.innerHTML = "";
        if (!items.length) { box.appendChild(el("div", { class: "muted", text: "Trash is empty. Deleted files are kept here for 30 days." })); return; }
        box.appendChild(el("div", { class: "muted", text: "Deleted files are kept for 30 days, then removed for good." }));
        var body = el("tbody");
        items.forEach(function (it) {
            body.appendChild(el("tr", null, [
                el("td", { text: it.originalPath }),
                el("td", { text: Bts.formatBytes(it.sizeBytes) }),
                el("td", { text: Bts.formatDate(it.deletedAt) }),
                el("td", null, [el("button", { type: "button", text: "Restore", onclick: async function () {
                    try {
                        await api.post("/api/bts/admin/spaces/" + current.id + "/trash/restore", { trashPath: it.trashPath });
                        Bts.toast("Restored to " + it.originalPath);
                        loadTrash();
                        refreshUsage();
                    } catch (e) { Bts.toast(e.message, true); }
                } })])
            ]));
        });
        box.appendChild(el("table", { class: "list" }, [
            el("thead", null, [el("tr", null, [el("th", { text: "File" }), el("th", { text: "Size" }), el("th", { text: "Deleted" }), el("th")])]),
            body
        ]));
    }

    async function loadActivity() {
        var box = $("tab-activity");
        box.textContent = "Loading…";
        var items;
        try {
            items = await api.get("/api/bts/admin/spaces/" + current.id + "/activity");
        } catch (e) { box.textContent = e.message; return; }
        box.innerHTML = "";
        if (!items.length) { box.appendChild(el("div", { class: "muted", text: "No activity yet." })); return; }
        var body = el("tbody");
        items.forEach(function (a) {
            body.appendChild(el("tr", null, [
                el("td", { text: Bts.formatDate(a.at) }),
                el("td", { text: a.actor }),
                el("td", { text: a.action }),
                el("td", { text: (a.path || "") + (a.newPath ? " \u2192 " + a.newPath : "") }),
                el("td", { text: a.bytes != null ? Bts.formatBytes(a.bytes) : "" }),
                el("td", { class: "muted", text: a.ip || "" })
            ]));
        });
        box.appendChild(el("table", { class: "list" }, [
            el("thead", null, [el("tr", null, ["When", "Who", "What", "Path", "Size", "IP"].map(function (h) { return el("th", { text: h }); }))]),
            body
        ]));
    }

    // ── Routing (#space/<id>) ──────────────────────────────────

    function route() {
        if (!session) return show("login");
        var m = /^#space\/(\d+)$/.exec(location.hash);
        if (m) openSpace(m[1]);
        else { closeSpace(); loadSpaces(); }
    }

    window.addEventListener("hashchange", route);
    route();
})();
