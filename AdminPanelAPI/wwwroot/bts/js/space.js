/* Customer page: the space link's token is the URL fragment (never sent to a server). */
(function () {
    "use strict";

    var token = decodeURIComponent((location.hash || "").replace(/^#/, ""));
    var browser = null;

    function deny(message) {
        document.getElementById("denied-message").textContent = message;
        document.getElementById("denied").classList.remove("hidden");
        document.getElementById("intro").classList.add("hidden");
        if (browser) { browser.destroy(); browser = null; }
    }

    var api = Bts.makeApi(function () { return token; }, deny);

    async function refreshUsage(info) {
        try {
            var u = await api.get("/api/bts/space/usage");
            var text = u.files + " files · " + Bts.formatBytes(u.bytes);
            if (u.quotaBytes) text += " of " + Bts.formatBytes(u.quotaBytes);
            document.getElementById("usage").textContent = text;
        } catch (e) { /* usage is informational */ }
    }

    async function start() {
        if (!token) return deny("Open the full link you were sent, including everything after the #.");
        var info;
        try {
            info = await api.get("/api/bts/space");
        } catch (e) {
            if (e.status !== 401) deny(e.message);
            return;
        }

        document.title = info.name + " · ShotDeck BTS";
        document.getElementById("space-name").textContent = info.name;
        var intro = "Upload behind-the-scenes photos and videos here. You can make folders, rename and delete your files. " +
            "Files up to " + Bts.formatBytes(info.maxFileBytes) + ".";
        if (info.expiresAt) intro += " This link works until " + Bts.formatDate(info.expiresAt) + ".";
        var introBox = document.getElementById("intro");
        introBox.textContent = intro;
        introBox.classList.remove("hidden");
        introBox.style.marginBottom = "14px";

        browser = new Bts.FileBrowser({
            root: document.getElementById("browser"),
            api: api,
            base: "/api/bts/space/files",
            onChange: refreshUsage
        });
        browser.fileInput.setAttribute("accept", info.allowedExtensions.join(","));
        await browser.load("");
        refreshUsage();
    }

    window.addEventListener("hashchange", function () { location.reload(); });
    start();
})();
