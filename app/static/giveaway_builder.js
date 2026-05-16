(() => {
  const RULE_CATALOG = {
    instagram: {
      comment_present: {label: "Comment present"},
      story_mention_present: {label: "Story mention present"},
      like_present: {label: "Like present"},
      repost_present: {label: "Repost present"},
      follow_present: {label: "Follow present"},
      friend_mention_count_gte: {label: "Friend mention count at least", params: [{key: "count", type: "number", label: "Count", min: 0}]},
      comment_keywords_all: {label: "Comment contains all keywords", params: [{key: "keywords", type: "list", label: "Keywords"}]},
      comment_hashtags_all: {label: "Comment contains all hashtags", params: [{key: "hashtags", type: "list", label: "Hashtags"}]},
    },
    bluesky: {
      reply_present: {label: "Reply present"},
      quote_present: {label: "Quote post present"},
      reply_or_quote_present: {label: "Reply or quote present"},
      reply_or_quote_mention_count_gte: {label: "Reply or quote @ mention count at least", params: [{key: "count", type: "number", label: "Count", min: 0}]},
      like_present: {label: "Like present"},
      follow_present: {label: "Follow present"},
      repost_present: {label: "Repost present"},
    },
  };

  function randomId(prefix) {
    return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function toDatetimeLocalValue(value) {
    if (!value) return "";
    const raw = String(value).trim().replace(" ", "T");
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(raw)) return raw;
    const normalized = raw.replace(/(\.\d{3})\d+/, "$1");
    const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(normalized);
    const date = new Date(hasZone ? normalized : `${normalized}Z`);
    if (Number.isNaN(date.getTime())) return "";
    const offset = date.getTimezoneOffset();
    const shifted = new Date(date.getTime() - (offset * 60 * 1000));
    return shifted.toISOString().slice(0, 16);
  }

  function parseList(value, prefix = "") {
    return String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => {
        if (!prefix) return item;
        return item.startsWith(prefix) ? item : `${prefix}${item.replaceAll(prefix, "")}`;
      });
  }

  function listToString(items) {
    return Array.isArray(items) ? items.join(", ") : "";
  }

  function defaultAtomForService(service) {
    if (service === "bluesky") return "reply_or_quote_present";
    return "comment_present";
  }

  function defaultRuleTree(service) {
    if (service === "bluesky") {
      return withIds({
        kind: "all",
        children: [
          {kind: "atom", atom: "reply_or_quote_present", params: {}},
          {kind: "atom", atom: "reply_or_quote_mention_count_gte", params: {count: 1}},
          {kind: "atom", atom: "like_present", params: {}},
          {kind: "atom", atom: "follow_present", params: {}},
        ],
      });
    }
    return withIds({
      kind: "all",
      children: [
        {kind: "atom", atom: "comment_present", params: {}},
        {kind: "atom", atom: "friend_mention_count_gte", params: {count: 1}},
        {kind: "atom", atom: "story_mention_present", params: {}},
      ],
    });
  }

  function defaultParams(service, atom) {
    const config = RULE_CATALOG[service]?.[atom];
    const params = {};
    (config?.params || []).forEach((definition) => {
      if (definition.type === "number") {
        params[definition.key] = definition.key === "count" ? 1 : 0;
      } else {
        params[definition.key] = [];
      }
    });
    return params;
  }

  function withIds(node) {
    const cloneNode = clone(node);
    const assign = (current) => {
      current._id = current._id || randomId("rule");
      current.kind = current.kind || "atom";
      current.params = current.params || {};
      current.children = Array.isArray(current.children) ? current.children : [];
      if (current.kind === "atom") {
        current.atom = current.atom || "";
        current.children = [];
      } else {
        current.atom = null;
      }
      current.children.forEach(assign);
      return current;
    };
    return assign(cloneNode);
  }

  function normalizeRuleNode(service, node) {
    const normalized = withIds(node || {kind: "atom", atom: defaultAtomForService(service), params: {}});
    if (normalized.kind === "atom") {
      const available = RULE_CATALOG[service] || {};
      if (!available[normalized.atom]) {
        normalized.atom = defaultAtomForService(service);
      }
      normalized.params = {...defaultParams(service, normalized.atom), ...(normalized.params || {})};
      normalized.children = [];
      return normalized;
    }
    normalized.children = normalized.children.map((child) => normalizeRuleNode(service, child));
    if (normalized.kind === "not") {
      normalized.children = normalized.children.slice(0, 1);
      if (!normalized.children.length) {
        normalized.children = [normalizeRuleNode(service, {kind: "atom", atom: defaultAtomForService(service), params: {}})];
      }
    }
    if ((normalized.kind === "all" || normalized.kind === "any") && !normalized.children.length) {
      normalized.children = [normalizeRuleNode(service, {kind: "atom", atom: defaultAtomForService(service), params: {}})];
    }
    return normalized;
  }

  function normalizeChannel(serviceAccounts, channel) {
    const service = channel?.service || "instagram";
    const accounts = serviceAccounts[service] || [];
    const accountIds = new Set(accounts.map((account) => account.id));
    return {
      _id: channel?._id || randomId("channel"),
      service,
      account_id: accountIds.has(channel?.account_id) ? channel.account_id : (accounts[0]?.id || ""),
      rules: normalizeRuleNode(service, channel?.rules || defaultRuleTree(service)),
    };
  }

  function catalogLabel(service, atom) {
    return RULE_CATALOG[service]?.[atom]?.label || atom.replaceAll("_", " ");
  }

  function summarizeRule(service, node) {
    if (!node) return "No rules yet.";
    if (node.kind === "atom") {
      if (node.atom === "friend_mention_count_gte" || node.atom === "reply_or_quote_mention_count_gte") {
        return `${catalogLabel(service, node.atom)} ${node.params?.count ?? 0}`;
      }
      if (node.atom === "comment_keywords_all") {
        return `${catalogLabel(service, node.atom)}: ${(node.params?.keywords || []).join(", ") || "none"}`;
      }
      if (node.atom === "comment_hashtags_all") {
        return `${catalogLabel(service, node.atom)}: ${(node.params?.hashtags || []).join(", ") || "none"}`;
      }
      return catalogLabel(service, node.atom);
    }
    const childSummaries = (node.children || []).map((child) => summarizeRule(service, child)).filter(Boolean);
    if (node.kind === "not") {
      return `NOT (${childSummaries[0] || "rule"})`;
    }
    return `${node.kind.toUpperCase()}: ${childSummaries.join(node.kind === "any" ? " OR " : " AND ")}`;
  }

  function serializeRule(node) {
    if (!node) return null;
    return {
      kind: node.kind,
      atom: node.kind === "atom" ? node.atom : null,
      params: node.kind === "atom" ? {...(node.params || {})} : {},
      children: node.kind === "atom" ? [] : (node.children || []).map(serializeRule),
    };
  }

  function findNode(root, targetId, parent = null) {
    if (!root) return null;
    if (root._id === targetId) {
      return {node: root, parent};
    }
    for (const child of root.children || []) {
      const found = findNode(child, targetId, root);
      if (found) return found;
    }
    return null;
  }

  function createAtomMarkup(service, node) {
    const options = Object.entries(RULE_CATALOG[service] || {})
      .map(([atom, config]) => `<option value="${atom}" ${node.atom === atom ? "selected" : ""}>${escapeHtml(config.label)}</option>`)
      .join("");
    const config = RULE_CATALOG[service]?.[node.atom] || {};
    const paramsMarkup = (config.params || []).map((definition) => {
      const value = node.params?.[definition.key];
      if (definition.type === "number") {
        return `
          <div class="col-md-4">
            <label class="form-label small-muted mb-1">${escapeHtml(definition.label)}</label>
            <input
              class="form-control form-control-sm"
              type="number"
              min="${definition.min ?? 0}"
              data-action="rule-param"
              data-node-id="${node._id}"
              data-param-key="${definition.key}"
              value="${escapeHtml(value ?? 0)}"
            >
          </div>
        `;
      }
      return `
        <div class="col-12">
          <label class="form-label small-muted mb-1">${escapeHtml(definition.label)}</label>
          <input
            class="form-control form-control-sm"
            type="text"
            placeholder="comma, separated, values"
            data-action="rule-param"
            data-node-id="${node._id}"
            data-param-key="${definition.key}"
            value="${escapeHtml(listToString(value))}"
          >
        </div>
      `;
    }).join("");
    return `
      <div class="border rounded p-3 bg-white" data-rule-node="${node._id}">
        <div class="row g-2 align-items-end">
          <div class="col-md-6">
            <label class="form-label small-muted mb-1">Rule</label>
            <select class="form-select form-select-sm" data-action="atom-change" data-node-id="${node._id}">
              ${options}
            </select>
          </div>
          <div class="col-md-6">
            <div class="d-flex gap-2 justify-content-md-end">
              <button class="btn btn-sm btn-outline-secondary" type="button" data-action="move-node-up" data-node-id="${node._id}">Up</button>
              <button class="btn btn-sm btn-outline-secondary" type="button" data-action="move-node-down" data-node-id="${node._id}">Down</button>
              <button class="btn btn-sm btn-outline-danger" type="button" data-action="remove-node" data-node-id="${node._id}">Remove</button>
            </div>
          </div>
          ${paramsMarkup}
        </div>
      </div>
    `;
  }

  function createGroupMarkup(service, node, isRoot = false) {
    const kindOptions = ["all", "any", "not"]
      .map((kind) => `<option value="${kind}" ${node.kind === kind ? "selected" : ""}>${kind.toUpperCase()}</option>`)
      .join("");
    const children = (node.children || [])
      .map((child) => renderRuleNode(service, child))
      .join("");
    return `
      <div class="border rounded p-3 bg-body-tertiary" data-rule-node="${node._id}">
        <div class="d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-2 mb-3">
          <div class="d-flex align-items-center gap-2">
            <label class="small-muted mb-0">Group</label>
            <select class="form-select form-select-sm" style="width: auto;" data-action="group-kind-change" data-node-id="${node._id}">
              ${kindOptions}
            </select>
          </div>
          <div class="d-flex gap-2 flex-wrap">
            <button class="btn btn-sm btn-outline-primary" type="button" data-action="add-atom" data-node-id="${node._id}">Add Rule</button>
            <button class="btn btn-sm btn-outline-secondary" type="button" data-action="add-group" data-node-id="${node._id}">Add Group</button>
            ${isRoot ? "" : `<button class="btn btn-sm btn-outline-secondary" type="button" data-action="move-node-up" data-node-id="${node._id}">Up</button>`}
            ${isRoot ? "" : `<button class="btn btn-sm btn-outline-secondary" type="button" data-action="move-node-down" data-node-id="${node._id}">Down</button>`}
            ${isRoot ? "" : `<button class="btn btn-sm btn-outline-danger" type="button" data-action="remove-node" data-node-id="${node._id}">Remove</button>`}
          </div>
        </div>
        <div class="d-flex flex-column gap-3">
          ${children}
        </div>
      </div>
    `;
  }

  function renderRuleNode(service, node, isRoot = false) {
    if (node.kind === "atom") {
      return createAtomMarkup(service, node);
    }
    return createGroupMarkup(service, node, isRoot);
  }

  function serviceAccountsFor(serviceAccounts, service) {
    return (serviceAccounts[service] || []).map((account) => ({
      id: account.id,
      label: account.label || account.handle_or_identifier || account.id,
      service: account.service,
    }));
  }

  function createBuilder({root, getServiceAccounts, initialValue, onChange, timezoneLabel}) {
    if (!root) {
      return {
        getValue() { return null; },
        getChannelAccountIds() { return []; },
        validate() { return {ok: true, message: ""}; },
        refresh() {},
      };
    }

    const initial = initialValue || {};
    const state = {
      giveaway_end_at: toDatetimeLocalValue(initial.giveaway_end_at),
      pool_mode: initial.pool_mode === "separate" ? "separate" : "combined",
      channels: [],
    };

    function currentAccounts() {
      const accounts = getServiceAccounts ? getServiceAccounts() : {};
      return {
        instagram: serviceAccountsFor(accounts, "instagram"),
        bluesky: serviceAccountsFor(accounts, "bluesky"),
      };
    }

    const api = {
      getValue() {
        normalizeState();
        return {
          giveaway_end_at: state.giveaway_end_at || null,
          pool_mode: state.pool_mode,
          channels: state.channels.map((channel) => ({
            service: channel.service,
            account_id: channel.account_id,
            rules: serializeRule(channel.rules),
          })),
        };
      },
      getChannelAccountIds() {
        normalizeState();
        return state.channels.map((channel) => channel.account_id).filter(Boolean);
      },
      validate() {
        const value = api.getValue();
        if (!value.giveaway_end_at) {
          return {ok: false, message: "Giveaway posts need an end time."};
        }
        if (!value.channels.length) {
          return {ok: false, message: "Giveaway posts need at least one channel."};
        }
        for (const channel of value.channels) {
          if (!channel.account_id) {
            return {ok: false, message: `Select a destination account for the ${channel.service} channel.`};
          }
        }
        return {ok: true, message: ""};
      },
      refresh() {
        render();
      },
    };

    function notifyChange() {
      if (typeof onChange === "function") onChange(api.getValue());
    }

    function normalizeState() {
      const accounts = currentAccounts();
      const seenServices = new Set();
      state.channels = state.channels
        .filter((channel) => !seenServices.has(channel.service) && (seenServices.add(channel.service), true))
        .map((channel) => normalizeChannel(accounts, channel))
        .filter((channel) => (accounts[channel.service] || []).length > 0);
    }

    function addChannel(service) {
      const accounts = currentAccounts();
      if (!accounts[service]?.length) return;
      if (state.channels.some((channel) => channel.service === service)) return;
      state.channels.push(normalizeChannel(accounts, {service, rules: defaultRuleTree(service)}));
      render();
    }

    function removeChannel(channelId) {
      state.channels = state.channels.filter((channel) => channel._id !== channelId);
      render();
    }

    function findChannel(channelId) {
      return state.channels.find((channel) => channel._id === channelId) || null;
    }

    function mutateNode(nodeId, callback) {
      for (const channel of state.channels) {
        const found = findNode(channel.rules, nodeId);
        if (found) {
          callback(found, channel);
          normalizeState();
          render();
          return;
        }
      }
    }

    function render() {
      normalizeState();
      const accounts = currentAccounts();
      const summary = state.channels.length
        ? state.channels.map((channel) => `${channel.service}: ${summarizeRule(channel.service, channel.rules)}`).join(" | ")
        : "No giveaway channels yet.";
      const addButtons = ["instagram", "bluesky"].map((service) => {
        const disabled = !accounts[service]?.length || state.channels.some((channel) => channel.service === service);
        const label = service === "instagram" ? "Instagram" : "Bluesky";
        return `<button class="btn btn-sm btn-outline-primary" type="button" data-action="add-channel" data-service="${service}" ${disabled ? "disabled" : ""}>Add ${label}</button>`;
      }).join("");
      root.innerHTML = `
        <div class="row g-3">
          <div class="col-md-6">
            <label class="form-label d-flex align-items-center gap-2 flex-wrap">
              <span>Giveaway Ends</span>
              <span class="badge text-bg-light border rounded-pill">${escapeHtml(timezoneLabel || "Profile timezone")}</span>
            </label>
            <input class="form-control" type="datetime-local" data-action="end-at" value="${escapeHtml(state.giveaway_end_at)}">
          </div>
          <div class="col-md-6">
            <label class="form-label">Pool Mode</label>
            <select class="form-select" data-action="pool-mode">
              <option value="combined" ${state.pool_mode === "combined" ? "selected" : ""}>Combined draw</option>
              <option value="separate" ${state.pool_mode === "separate" ? "selected" : ""}>Separate draw per channel</option>
            </select>
          </div>
        </div>
        <div class="surface-panel-soft mt-3">
          <div class="d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3">
            <div>
              <div class="fw-semibold">Channels</div>
              <div class="small text-secondary">Enable Instagram, Bluesky, or both, then build qualifying rules visually.</div>
            </div>
            <div class="d-flex gap-2 flex-wrap">${addButtons}</div>
          </div>
          <div class="small text-secondary mt-3" data-giveaway-summary>${escapeHtml(summary)}</div>
        </div>
        <div class="d-flex flex-column gap-3 mt-3">
          ${state.channels.map((channel) => {
            const accountOptions = (accounts[channel.service] || [])
              .map((account) => `<option value="${account.id}" ${channel.account_id === account.id ? "selected" : ""}>${escapeHtml(account.label)}</option>`)
              .join("");
            const label = channel.service === "instagram" ? "Instagram" : "Bluesky";
            return `
              <div class="border rounded p-3 bg-white" data-channel-id="${channel._id}">
                <div class="d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 mb-3">
                  <div>
                    <div class="fw-semibold">${label} Channel</div>
                    <div class="small text-secondary">${escapeHtml(summarizeRule(channel.service, channel.rules))}</div>
                  </div>
                  <button class="btn btn-sm btn-outline-danger" type="button" data-action="remove-channel" data-channel-id="${channel._id}">Remove Channel</button>
                </div>
                <div class="row g-3 mb-3">
                  <div class="col-md-6">
                    <label class="form-label">Destination Account</label>
                    <select class="form-select" data-action="channel-account" data-channel-id="${channel._id}">
                      ${accountOptions}
                    </select>
                  </div>
                </div>
                ${renderRuleNode(channel.service, channel.rules, true)}
              </div>
            `;
          }).join("")}
        </div>
      `;
      notifyChange();
    }

    function handleClick(event) {
      const button = event.target.closest("[data-action]");
      if (!button) return;
      const action = button.dataset.action;
      if (action === "add-channel") {
        addChannel(button.dataset.service);
        return;
      }
      if (action === "remove-channel") {
        removeChannel(button.dataset.channelId);
        return;
      }
      if (action === "add-atom" || action === "add-group") {
        mutateNode(button.dataset.nodeId, ({node}) => {
          if (node.kind === "atom") return;
          const child = action === "add-group"
            ? {kind: "all", children: [{kind: "atom", atom: defaultAtomForService(findServiceForNode(button.dataset.nodeId)), params: {}}]}
            : {kind: "atom", atom: defaultAtomForService(findServiceForNode(button.dataset.nodeId)), params: {}};
          node.children.push(normalizeRuleNode(findServiceForNode(button.dataset.nodeId), child));
        });
        return;
      }
      if (action === "remove-node") {
        mutateNode(button.dataset.nodeId, ({node, parent}, channel) => {
          if (!parent) return;
          parent.children = (parent.children || []).filter((child) => child._id !== node._id);
          if (!parent.children.length && parent.kind !== "not") {
            parent.children = [normalizeRuleNode(channel.service, {kind: "atom", atom: defaultAtomForService(channel.service), params: {}})];
          }
          if (parent.kind === "not" && !parent.children.length) {
            parent.children = [normalizeRuleNode(channel.service, {kind: "atom", atom: defaultAtomForService(channel.service), params: {}})];
          }
        });
        return;
      }
      if (action === "move-node-up" || action === "move-node-down") {
        mutateNode(button.dataset.nodeId, ({node, parent}) => {
          if (!parent) return;
          const siblings = parent.children || [];
          const index = siblings.findIndex((child) => child._id === node._id);
          if (index < 0) return;
          const nextIndex = action === "move-node-up" ? index - 1 : index + 1;
          if (nextIndex < 0 || nextIndex >= siblings.length) return;
          [siblings[index], siblings[nextIndex]] = [siblings[nextIndex], siblings[index]];
        });
      }
    }

    function findServiceForNode(nodeId) {
      const channel = state.channels.find((item) => findNode(item.rules, nodeId));
      return channel?.service || "instagram";
    }

    function handleChange(event) {
      const action = event.target.dataset.action;
      if (!action) return;
      if (action === "end-at") {
        state.giveaway_end_at = event.target.value || "";
        notifyChange();
        return;
      }
      if (action === "pool-mode") {
        state.pool_mode = event.target.value === "separate" ? "separate" : "combined";
        notifyChange();
        return;
      }
      if (action === "channel-account") {
        const channel = findChannel(event.target.dataset.channelId);
        if (!channel) return;
        channel.account_id = event.target.value || "";
        notifyChange();
        return;
      }
      if (action === "group-kind-change") {
        mutateNode(event.target.dataset.nodeId, ({node}, channel) => {
          node.kind = event.target.value;
          node.atom = null;
          node.params = {};
          node.children = (node.children || []).map((child) => normalizeRuleNode(channel.service, child));
        });
        return;
      }
      if (action === "atom-change") {
        mutateNode(event.target.dataset.nodeId, ({node}, channel) => {
          node.kind = "atom";
          node.atom = event.target.value;
          node.params = defaultParams(channel.service, node.atom);
          node.children = [];
        });
        return;
      }
      if (action === "rule-param") {
        mutateNode(event.target.dataset.nodeId, ({node}, channel) => {
          const key = event.target.dataset.paramKey;
          const definition = (RULE_CATALOG[channel.service]?.[node.atom]?.params || []).find((item) => item.key === key);
          if (!definition) return;
          if (definition.type === "number") {
            node.params[key] = Number.parseInt(event.target.value || "0", 10) || 0;
          } else if (definition.key === "hashtags") {
            node.params[key] = parseList(event.target.value, "#");
          } else {
            node.params[key] = parseList(event.target.value);
          }
        });
      }
    }

    root.addEventListener("click", handleClick);
    root.addEventListener("change", handleChange);
    root.addEventListener("input", handleChange);

    state.channels = (initial.channels || []).map((channel) => ({
      _id: randomId("channel"),
      service: channel.service,
      account_id: channel.account_id,
      rules: withIds(channel.rules || defaultRuleTree(channel.service)),
    }));
    normalizeState();
    render();
    return api;
  }

  window.LynxGiveawayBuilder = {
    create: createBuilder,
    defaultRuleTree,
    catalog: RULE_CATALOG,
  };
})();
