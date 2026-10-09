/* Shared helpers: API calls, formatting, small dialogs. */
(function () {
    "use strict";

    function ApiError(status, message) {
        this.status = status;
        this.message = message;
    }
    ApiError.prototype = Object.create(Error.prototype);

    /** Makes a JSON API client that sends the given bearer token. */
    function makeApi(getToken, onUnauthorized) {
        async function call(method, url, body) {
            var headers = { "Accept": "application/json" };
            var token = getToken();
            if (token) headers["Authorization"] = "Bearer " + token;
            if (body !== undefined) headers["Content-Type"] = "application/json";

            var res;
            try {
                res = await fetch((window.BTS_API_BASE || "") + url, {
                    method: method,
                    headers: headers,
                    body: body === undefined ? undefined : JSON.stringify(body),
                    credentials: "omit",
                    referrerPolicy: "no-referrer"
                });
            } catch (e) {
                throw new ApiError(0, "Can't reach the server. Check your connection and try again.");
            }

            if (res.status === 204) return null;
            var data = null;
            try { data = await res.json(); } catch (e) { /* empty body */ }

            if (!res.ok) {
                var message = (data && data.error) || (res.status === 429
                    ? "Too many requests. Wait a minute and try again."
                    : "Request failed (" + res.status + ").");
                if (res.status === 401 && onUnauthorized) onUnauthorized(message);
                throw new ApiError(res.status, message);
            }
            return data;
        }

        return {
            get: function (url) { return call("GET", url); },
            post: function (url, body) { return call("POST", url, body || {}); },
            put: function (url, body) { return call("PUT", url, body || {}); },
            del: function (url) { return call("DELETE", url); }
        };
    }

    function formatBytes(bytes) {
        if (bytes == null) return "";
        var units = ["B", "KB", "MB", "GB", "TB"];
        var v = bytes, u = 0;
        while (v >= 1024 && u < units.length - 1) { v /= 1024; u++; }
        return (u === 0 ? v : v.toFixed(v < 10 ? 1 : 0)) + " " + units[u];
    }

    function formatDate(iso) {
        if (!iso) return "";
        var d = new Date(iso);
        return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }) +
            " " + d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
    }

    function el(tag, attrs, children) {
        var node = document.createElement(tag);
        if (attrs) {
            Object.keys(attrs).forEach(function (k) {
                var v = attrs[k];
                if (v == null || v === false) return;
                if (k === "text") node.textContent = v;
                else if (k === "class") node.className = v;
                else if (k.indexOf("on") === 0) node.addEventListener(k.slice(2), v);
                else node.setAttribute(k, v === true ? "" : v);
            });
        }
        (children || []).forEach(function (c) {
            if (c == null) return;
            node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
        });
        return node;
    }

    /** A small modal form. fields: [{name, label, value, type, placeholder}]. Resolves to values or null. */
    function promptDialog(title, fields, okLabel, danger) {
        return new Promise(function (resolve) {
            var inputs = {};
            var errorBox = el("div", { class: "error" });
            var form = el("form", { method: "dialog", class: "stack" }, [el("h3", { text: title })]);
            fields.forEach(function (f) {
                var input = f.type === "textarea"
                    ? el("textarea", { rows: 5, placeholder: f.placeholder || "", maxlength: f.maxlength })
                    : el("input", { type: f.type || "text", placeholder: f.placeholder || "", step: f.step });
                input.value = f.value == null ? "" : f.value;
                inputs[f.name] = input;
                form.appendChild(el("label", null, [f.label, input]));
            });
            if (fields.length === 0 && danger) form.appendChild(el("div", { class: "muted", text: danger }));
            form.appendChild(errorBox);
            var cancel = el("button", { type: "button", text: "Cancel" });
            var ok = el("button", { type: "submit", class: "primary", text: okLabel || "OK" });
            form.appendChild(el("div", { class: "row" }, [cancel, ok]));

            var dlg = el("dialog", null, [form]);
            document.body.appendChild(dlg);
            function close(result) { dlg.close(); dlg.remove(); resolve(result); }
            cancel.addEventListener("click", function () { close(null); });
            dlg.addEventListener("cancel", function (e) { e.preventDefault(); close(null); });
            form.addEventListener("submit", function (e) {
                e.preventDefault();
                var values = {};
                Object.keys(inputs).forEach(function (k) { values[k] = inputs[k].value; });
                close(values);
            });
            dlg.showModal();
            var first = fields.length ? inputs[fields[0].name] : ok;
            first.focus();
            if (first.select && fields.length && fields[0].selectBase) {
                var dot = first.value.lastIndexOf(".");
                first.setSelectionRange(0, dot > 0 ? dot : first.value.length);
            }
        });
    }

    function confirmDialog(title, message, okLabel) {
        return promptDialog(title, [], okLabel || "OK", message).then(function (v) { return v !== null; });
    }

    function toast(message, isError) {
        var box = document.getElementById("toast");
        if (!box) {
            box = el("div", { id: "toast" });
            box.style.cssText = "position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:40;" +
                "padding:10px 16px;border-radius:8px;background:#22262e;border:1px solid #2e333d;max-width:80vw";
            document.body.appendChild(box);
        }
        box.textContent = message;
        box.style.color = isError ? "#e5534b" : "#e8eaed";
        box.style.display = "block";
        clearTimeout(box._t);
        box._t = setTimeout(function () { box.style.display = "none"; }, isError ? 6000 : 3000);
    }

    window.Bts = {
        ApiError: ApiError,
        makeApi: makeApi,
        formatBytes: formatBytes,
        formatDate: formatDate,
        el: el,
        promptDialog: promptDialog,
        confirmDialog: confirmDialog,
        toast: toast
    };
})();
