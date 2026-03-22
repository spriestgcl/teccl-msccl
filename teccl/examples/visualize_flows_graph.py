import argparse
import json
import re
from pathlib import Path


FLOW_RE = re.compile(
    r"Chunk (?P<chunk>\d+) from (?P<source>\d+) traveled over "
    r"(?P<sender>\d+)->(?P<receiver>\d+) in epoch (?P<epoch>\d+)"
    r"(?: via switches (?P<switches>.*))?"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize TECCL 7-Flows as an interactive graph HTML page."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TECCL schedule JSON.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output HTML. Defaults to input path with _graph.html suffix.",
    )
    parser.add_argument(
        "--split-at",
        type=int,
        default=None,
        help="Split point for two-chassis view. Example: 8 for nodes 0-7 and 8-15.",
    )
    return parser.parse_args()


def parse_flows(schedule_json):
    flows = []
    for flow_str in schedule_json.get("7-Flows", []):
        match = FLOW_RE.match(flow_str)
        if not match:
            raise ValueError(f"Unsupported flow format: {flow_str}")
        raw = match.groupdict()
        switches = raw.get("switches")
        switch_list = []
        if switches:
            switch_list = [part.strip() for part in switches.split("->") if part.strip()]
        flows.append(
            {
                "chunk": int(raw["chunk"]),
                "source": int(raw["source"]),
                "sender": int(raw["sender"]),
                "receiver": int(raw["receiver"]),
                "epoch": int(raw["epoch"]),
                "switches": switch_list,
                "label": flow_str,
            }
        )
    if not flows:
        raise ValueError("Input schedule JSON does not contain 7-Flows.")
    return flows


def choose_split(nodes, split_at):
    if split_at is not None:
        return split_at
    if len(nodes) in (15, 16):
        return 8
    return max(1, len(nodes) // 2)


def compute_positions(nodes, split_at):
    width = 1500
    height = 860
    positions = {}
    left_box = {"x": 90, "y": 120, "w": 450, "h": 610}
    right_box = {"x": 760, "y": 120, "w": 450, "h": 610}

    left_nodes = [n for n in nodes if n < split_at]
    right_nodes = [n for n in nodes if n >= split_at]

    def place(group_nodes, box):
        cols_x = [box["x"] + 135, box["x"] + box["w"] - 135]
        rows_y = [box["y"] + 85, box["y"] + 220, box["y"] + 355, box["y"] + 490]
        for idx, node in enumerate(sorted(group_nodes)):
            local = idx if node < split_at else node - split_at
            if 0 <= local < 8:
                col = 0 if local < 4 else 1
                row = local % 4
            else:
                col = idx % 2
                row = (idx // 2) % 4
            positions[node] = {"x": cols_x[col], "y": rows_y[row]}

    place(left_nodes, left_box)
    place(right_nodes, right_box)
    return {
        "width": width,
        "height": height,
        "nodes": positions,
        "left_box": left_box,
        "right_box": right_box,
        "bridge_x1": left_box["x"] + left_box["w"] + 85,
        "bridge_x2": right_box["x"] - 85,
    }


def build_model(schedule_json, flows, split_at):
    nodes = sorted(
        {
            node
            for flow in flows
            for node in (flow["source"], flow["sender"], flow["receiver"])
        }
    )
    split_at = choose_split(nodes, split_at)
    positions = compute_positions(nodes, split_at)
    epochs_required = int(schedule_json.get("3-Epochs_Required", max(f["epoch"] for f in flows) + 1))
    flows_by_epoch = {epoch: [] for epoch in range(epochs_required)}
    for flow in flows:
        flow["cross_group"] = ((flow["sender"] < split_at) != (flow["receiver"] < split_at))
        flow["switch_path"] = bool(flow["switches"])
        flows_by_epoch.setdefault(flow["epoch"], []).append(flow)
    for epoch in flows_by_epoch:
        flows_by_epoch[epoch].sort(key=lambda f: (f["sender"], f["receiver"], f["source"], f["chunk"]))

    return {
        "title": Path(schedule_json.get("InstanceParams", {}).get("schedule_output_file", "")).name
        or "TECCL 7-Flows",
        "nodes": nodes,
        "split_at": split_at,
        "positions": positions,
        "epochs_required": epochs_required,
        "epoch_duration": schedule_json.get("1-Epoch_Duration"),
        "algo_bandwidth": schedule_json.get("5-Algo_Bandwidth"),
        "flow_count": len(flows),
        "cross_group_flow_count": sum(1 for flow in flows if flow["cross_group"]),
        "switch_path_flow_count": sum(1 for flow in flows if flow["switch_path"]),
        "flows_by_epoch": flows_by_epoch,
    }


def render_html(model):
    data_json = json.dumps(model, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TECCL 7-Flows Graph Viewer</title>
  <style>
    :root {{
      --bg: #f4f0e8;
      --panel: #fffdf8;
      --ink: #1d2430;
      --muted: #69707d;
      --line: #d8cfbf;
      --accent: #9b3d23;
      --edge: #2e6ea7;
      --edge-cross: #b63e2a;
      --edge-switch: #d28a12;
      --node: #203f5a;
      --node-active: #0e7490;
      --soft-blue: #e8f1f8;
      --soft-sand: #f7efe3;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff9ee 0, transparent 24%),
        radial-gradient(circle at bottom right, #ede4d7 0, transparent 30%),
        var(--bg);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
    }}
    .app {{
      display: grid;
      grid-template-columns: 330px 1fr 360px;
      gap: 18px;
      min-height: 100vh;
      padding: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 14px 34px rgba(54, 43, 27, 0.08);
      min-width: 0;
    }}
    .sidebar, .detail {{
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }}
    .main {{
      padding: 18px;
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 14px;
      min-width: 0;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      line-height: 1.1;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }}
    .stats {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .stat {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255,255,255,0.75);
    }}
    .stat strong {{
      display: block;
      font-size: 22px;
      color: var(--accent);
    }}
    .controls {{
      display: grid;
      gap: 10px;
    }}
    label {{
      font-size: 13px;
      color: var(--muted);
      display: grid;
      gap: 4px;
    }}
    select, input[type="range"], button {{
      width: 100%;
    }}
    select, button {{
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      border-radius: 10px;
      padding: 9px 10px;
      font: inherit;
    }}
    button {{
      cursor: pointer;
      font-weight: 600;
    }}
    .button-row {{
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 8px;
    }}
    .button-row2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }}
    .primary {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }}
    .epoch-strip {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(52px, 1fr));
      gap: 8px;
    }}
    .epoch-pill {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 8px 6px;
      text-align: center;
      background: white;
      cursor: pointer;
      font: inherit;
    }}
    .epoch-pill.active {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }}
    .epoch-pill .count {{
      display: block;
      font-size: 18px;
      font-weight: 700;
    }}
    .canvas-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.94), rgba(247,239,227,0.96));
      min-height: 0;
    }}
    svg {{
      width: 100%;
      height: auto;
      min-height: 760px;
      display: block;
    }}
    .legend {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      font-size: 13px;
      color: var(--muted);
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 18px;
      height: 3px;
      margin-right: 6px;
      vertical-align: middle;
      border-radius: 999px;
      background: var(--legend-color, var(--edge));
    }}
    .node-circle {{
      fill: var(--node);
      stroke: white;
      stroke-width: 4;
      cursor: pointer;
    }}
    .node-circle.active {{
      fill: var(--node-active);
    }}
    .node-label {{
      fill: white;
      font-size: 15px;
      font-weight: 700;
      text-anchor: middle;
      dominant-baseline: middle;
      pointer-events: none;
    }}
    .node-side {{
      fill: #667085;
      font-size: 12px;
      text-anchor: middle;
      pointer-events: none;
    }}
    .edge-label {{
      font-size: 11px;
      font-weight: 700;
      fill: #3a332a;
      text-anchor: middle;
      cursor: pointer;
    }}
    .edge-hit {{
      stroke: transparent;
      stroke-width: 20;
      fill: none;
      cursor: pointer;
    }}
    .edge-summary {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: rgba(255,255,255,0.75);
    }}
    .flow-list {{
      display: grid;
      gap: 8px;
      max-height: 520px;
      overflow: auto;
    }}
    .flow-item {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      background: rgba(255,255,255,0.82);
      font-size: 13px;
      line-height: 1.45;
    }}
    code {{
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
    }}
    @media (max-width: 1450px) {{
      .app {{ grid-template-columns: 320px 1fr; }}
      .detail {{ grid-column: 1 / -1; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="panel sidebar">
      <div>
        <h1>7-Flows Graph</h1>
        <div class="subtle">这版专门针对双机箱节点图。边不再贴满文字，而是尽量避开重叠，详情统一放到右侧。</div>
      </div>
      <div class="stats">
        <div class="stat"><strong id="statNodes"></strong><span>Nodes</span></div>
        <div class="stat"><strong id="statFlows"></strong><span>Total Flows</span></div>
        <div class="stat"><strong id="statEpochs"></strong><span>Epochs</span></div>
        <div class="stat"><strong id="statViewFlows"></strong><span>Flows In View</span></div>
      </div>
      <div id="meta" class="subtle"></div>
      <div class="controls">
        <label>Epoch
          <input id="epochSlider" type="range" min="0" max="0" value="0">
        </label>
        <div id="epochReadout" class="subtle"></div>
        <div class="button-row">
          <button id="prevBtn">Prev</button>
          <button id="playBtn" class="primary">Play</button>
          <button id="nextBtn">Next</button>
        </div>
        <div class="button-row2">
          <button id="modeBtn">Show Prefix 0..epoch</button>
          <button id="resetBtn">Reset Filters</button>
        </div>
        <label>Source Filter
          <select id="sourceFilter"></select>
        </label>
        <label>Sender Filter
          <select id="senderFilter"></select>
        </label>
        <label>Receiver Filter
          <select id="receiverFilter"></select>
        </label>
        <label>Edge Filter
          <select id="edgeFilter">
            <option value="all">All edges</option>
            <option value="cross">Cross-chassis only</option>
            <option value="intra">Intra-chassis only</option>
          </select>
        </label>
      </div>
      <div class="subtle">
        使用建议：
        <br>1. 先看单个 epoch
        <br>2. 点某个节点，只看跟它相连的边
        <br>3. 点某条边，在右侧看该边承载的具体 flow
        <br>4. epoch 0 太密时，用 sender/source filter 收缩视图
      </div>
    </aside>

    <main class="panel main">
      <div>
        <div style="font-size:30px;font-weight:700;" id="epochTitle"></div>
        <div class="subtle" id="epochSummary"></div>
      </div>
      <div id="epochStrip" class="epoch-strip"></div>
      <div class="canvas-wrap">
        <svg id="canvas" viewBox="0 0 1500 860"></svg>
      </div>
      <div class="legend">
        <span style="--legend-color:var(--edge);">Intra-chassis</span>
        <span style="--legend-color:var(--edge-cross);">Cross-chassis</span>
        <span style="--legend-color:var(--edge-switch);">Has via switches</span>
      </div>
    </main>

    <aside class="panel detail">
      <div class="edge-summary">
        <strong id="detailTitle">No edge selected</strong>
        <div id="detailMeta" class="subtle" style="margin-top:8px;">点击一条边后，这里会列出该边上的所有 flow。</div>
      </div>
      <div id="flowList" class="flow-list"></div>
    </aside>
  </div>

  <script>
    const MODEL = {data_json};
    const svg = document.getElementById("canvas");
    const epochSlider = document.getElementById("epochSlider");
    const epochTitle = document.getElementById("epochTitle");
    const epochReadout = document.getElementById("epochReadout");
    const epochSummary = document.getElementById("epochSummary");
    const epochStrip = document.getElementById("epochStrip");
    const meta = document.getElementById("meta");
    const statNodes = document.getElementById("statNodes");
    const statFlows = document.getElementById("statFlows");
    const statEpochs = document.getElementById("statEpochs");
    const statViewFlows = document.getElementById("statViewFlows");
    const sourceFilter = document.getElementById("sourceFilter");
    const senderFilter = document.getElementById("senderFilter");
    const receiverFilter = document.getElementById("receiverFilter");
    const edgeFilter = document.getElementById("edgeFilter");
    const detailTitle = document.getElementById("detailTitle");
    const detailMeta = document.getElementById("detailMeta");
    const flowList = document.getElementById("flowList");

    let currentEpoch = 0;
    let showPrefix = false;
    let timer = null;
    let selectedNode = null;
    let selectedEdgeKey = null;

    epochSlider.max = Math.max(0, MODEL.epochs_required - 1);
    statNodes.textContent = MODEL.nodes.length;
    statFlows.textContent = MODEL.flow_count;
    statEpochs.textContent = MODEL.epochs_required;

    const metaParts = [];
    metaParts.push(`cross_chassis_flows=${{MODEL.cross_group_flow_count}}`);
    metaParts.push(`switch_path_flows=${{MODEL.switch_path_flow_count}}`);
    if (MODEL.epoch_duration !== null && MODEL.epoch_duration !== undefined) {{
      metaParts.push(`epoch_duration=${{MODEL.epoch_duration}}`);
    }}
    if (MODEL.algo_bandwidth !== null && MODEL.algo_bandwidth !== undefined) {{
      metaParts.push(`algo_bandwidth=${{MODEL.algo_bandwidth}}`);
    }}
    meta.textContent = metaParts.join(" | ");

    function fillNodeSelect(selectEl, title) {{
      selectEl.innerHTML = [`<option value="">${{title}}</option>`]
        .concat(MODEL.nodes.map((node) => `<option value="${{node}}">GPU ${{node}}</option>`))
        .join("");
    }}

    fillNodeSelect(sourceFilter, "All sources");
    fillNodeSelect(senderFilter, "All senders");
    fillNodeSelect(receiverFilter, "All receivers");

    function inFirstChassis(node) {{
      return node < MODEL.split_at;
    }}

    function pairKey(sender, receiver) {{
      return `${{sender}}->${{receiver}}`;
    }}

    function collectEpochFlows(epoch) {{
      const flows = [];
      if (showPrefix) {{
        for (let e = 0; e <= epoch; e++) {{
          flows.push(...(MODEL.flows_by_epoch[e] || []));
        }}
      }} else {{
        flows.push(...(MODEL.flows_by_epoch[epoch] || []));
      }}
      return flows;
    }}

    function getFilteredFlows() {{
      const srcVal = sourceFilter.value;
      const sendVal = senderFilter.value;
      const recvVal = receiverFilter.value;
      const edgeMode = edgeFilter.value;

      return collectEpochFlows(currentEpoch).filter((flow) => {{
        if (srcVal !== "" && flow.source !== Number(srcVal)) return false;
        if (sendVal !== "" && flow.sender !== Number(sendVal)) return false;
        if (recvVal !== "" && flow.receiver !== Number(recvVal)) return false;
        if (selectedNode !== null && flow.sender !== selectedNode && flow.receiver !== selectedNode) return false;
        if (edgeMode === "cross" && !flow.cross_group) return false;
        if (edgeMode === "intra" && flow.cross_group) return false;
        return true;
      }});
    }}

    function buildEdges(flows) {{
      const grouped = new Map();
      for (const flow of flows) {{
        const key = pairKey(flow.sender, flow.receiver);
        if (!grouped.has(key)) {{
          grouped.set(key, {{
            key,
            sender: flow.sender,
            receiver: flow.receiver,
            flows: [],
            cross: flow.cross_group,
            hasSwitch: false,
          }});
        }}
        const edge = grouped.get(key);
        edge.flows.push(flow);
        if (flow.switch_path) edge.hasSwitch = true;
      }}
      return Array.from(grouped.values()).sort((a, b) => {{
        if (a.cross !== b.cross) return a.cross ? 1 : -1;
        if (a.sender !== b.sender) return a.sender - b.sender;
        return a.receiver - b.receiver;
      }});
    }}

    function buildEpochStrip() {{
      const pills = [];
      for (let e = 0; e < MODEL.epochs_required; e++) {{
        const count = (MODEL.flows_by_epoch[e] || []).length;
        pills.push(`
          <button class="epoch-pill ${{e === currentEpoch ? "active" : ""}}" data-epoch="${{e}}">
            <span>E${{e}}</span>
            <span class="count">${{count}}</span>
          </button>
        `);
      }}
      epochStrip.innerHTML = pills.join("");
      for (const btn of epochStrip.querySelectorAll(".epoch-pill")) {{
        btn.addEventListener("click", () => {{
          currentEpoch = Number(btn.dataset.epoch);
          epochSlider.value = currentEpoch;
          selectedEdgeKey = null;
          render();
        }});
      }}
    }}

    function cubicPath(edge) {{
      const p1 = MODEL.positions.nodes[edge.sender];
      const p2 = MODEL.positions.nodes[edge.receiver];
      if (edge.cross) {{
        const bridgeX1 = MODEL.positions.bridge_x1;
        const bridgeX2 = MODEL.positions.bridge_x2;
        const laneBase = 90;
        const laneSpan = 600;
        const laneSeed = ((edge.sender % MODEL.split_at) * 8) + (edge.receiver % MODEL.split_at);
        const laneY = laneBase + (laneSeed % 12) * (laneSpan / 11);
        return {{
          d: `M ${{p1.x}} ${{p1.y}} C ${{bridgeX1}} ${{p1.y}}, ${{bridgeX1}} ${{laneY}}, ${{(bridgeX1 + bridgeX2) / 2}} ${{laneY}}
              C ${{bridgeX2}} ${{laneY}}, ${{bridgeX2}} ${{p2.y}}, ${{p2.x}} ${{p2.y}}`,
          lx: (bridgeX1 + bridgeX2) / 2,
          ly: laneY - 8,
        }};
      }}
      const leftSide = inFirstChassis(edge.sender) && inFirstChassis(edge.receiver);
      const outwardX = leftSide ? 40 : 1260;
      const offsetSeed = (edge.receiver - edge.sender);
      const offsetY = Math.max(-120, Math.min(120, offsetSeed * 16));
      const c1x = leftSide ? outwardX : outwardX;
      const c2x = leftSide ? outwardX : outwardX;
      const c1y = p1.y + offsetY;
      const c2y = p2.y - offsetY;
      return {{
        d: `M ${{p1.x}} ${{p1.y}} C ${{c1x}} ${{c1y}}, ${{c2x}} ${{c2y}}, ${{p2.x}} ${{p2.y}}`,
        lx: (p1.x + p2.x + c1x + c2x) / 4,
        ly: (p1.y + p2.y + c1y + c2y) / 4 - 8,
      }};
    }}

    function renderDetails(edge, flows) {{
      if (!edge) {{
        detailTitle.textContent = "No edge selected";
        detailMeta.textContent = "点击一条边后，这里会列出该边上的所有 flow。";
        flowList.innerHTML = "";
        return;
      }}
      detailTitle.textContent = `Edge ${{edge.sender}} -> ${{edge.receiver}}`;
      detailMeta.textContent = `flow_count=${{edge.flows.length}} | cross_chassis=${{edge.cross}} | has_switch_path=${{edge.hasSwitch}}`;
      flowList.innerHTML = edge.flows.map((flow, idx) => `
        <div class="flow-item">
          <div><strong>#${{idx + 1}}</strong> epoch=${{flow.epoch}} src=${{flow.source}} chunk=${{flow.chunk}}</div>
          <div>sender=${{flow.sender}} receiver=${{flow.receiver}}</div>
          <div>switches=${{flow.switches.join(" -> ") || "none"}}</div>
          <div><code>${{flow.label}}</code></div>
        </div>
      `).join("");
    }}

    function renderGraph(edges) {{
      const parts = [];
      parts.push(`<defs>
        <marker id="arrow-blue" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="#2e6ea7"></path>
        </marker>
        <marker id="arrow-red" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="#b63e2a"></path>
        </marker>
        <marker id="arrow-orange" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="#d28a12"></path>
        </marker>
      </defs>`);

      const leftBox = MODEL.positions.left_box;
      const rightBox = MODEL.positions.right_box;
      parts.push(`<rect x="${{leftBox.x}}" y="${{leftBox.y}}" width="${{leftBox.w}}" height="${{leftBox.h}}" rx="28" fill="${{ "#f7efe3" }}" stroke="#ddcfbb" stroke-width="2"></rect>`);
      parts.push(`<rect x="${{rightBox.x}}" y="${{rightBox.y}}" width="${{rightBox.w}}" height="${{rightBox.h}}" rx="28" fill="${{ "#e8f1f8" }}" stroke="#c8d8e6" stroke-width="2"></rect>`);
      parts.push(`<text x="${{leftBox.x + leftBox.w / 2}}" y="82" font-size="26" text-anchor="middle" fill="#9d8561" font-weight="700">Chassis 0</text>`);
      parts.push(`<text x="${{rightBox.x + rightBox.w / 2}}" y="82" font-size="26" text-anchor="middle" fill="#6b89a7" font-weight="700">Chassis 1</text>`);
      parts.push(`<text x="${{leftBox.x + leftBox.w / 2}}" y="104" font-size="13" text-anchor="middle" fill="#90785b">GPU 0 .. ${{MODEL.split_at - 1}}</text>`);
      parts.push(`<text x="${{rightBox.x + rightBox.w / 2}}" y="104" font-size="13" text-anchor="middle" fill="#6f89a2">GPU ${{MODEL.split_at}} .. ${{MODEL.nodes[MODEL.nodes.length - 1]}}</text>`);

      for (const edge of edges) {{
        const path = cubicPath(edge);
        const color = edge.hasSwitch ? "#d28a12" : (edge.cross ? "#b63e2a" : "#2e6ea7");
        const marker = edge.hasSwitch ? "url(#arrow-orange)" : (edge.cross ? "url(#arrow-red)" : "url(#arrow-blue)");
        const width = Math.min(10, 2 + edge.flows.length * 0.9);
        const opacity = selectedEdgeKey && selectedEdgeKey !== edge.key ? 0.18 : 0.86;
        const dash = edge.hasSwitch ? "7 5" : "none";
        parts.push(`<path d="${{path.d}}" fill="none" stroke="${{color}}" stroke-width="${{width}}" stroke-linecap="round" marker-end="${{marker}}" opacity="${{opacity}}" stroke-dasharray="${{dash}}"></path>`);
        parts.push(`<path class="edge-hit" d="${{path.d}}" data-edge-key="${{edge.key}}"></path>`);
        if (!selectedNode || edge.sender === selectedNode || edge.receiver === selectedNode || selectedEdgeKey === edge.key) {{
          parts.push(`<text class="edge-label" x="${{path.lx}}" y="${{path.ly}}" opacity="${{opacity}}">${{edge.sender}}->${{edge.receiver}} (${{edge.flows.length}})</text>`);
        }}
      }}

      for (const node of MODEL.nodes) {{
        const p = MODEL.positions.nodes[node];
        const active = selectedNode === node ? " active" : "";
        const side = inFirstChassis(node) ? "C0" : "C1";
        parts.push(`<circle class="node-circle${{active}}" data-node-id="${{node}}" cx="${{p.x}}" cy="${{p.y}}" r="28"></circle>`);
        parts.push(`<text class="node-label" x="${{p.x}}" y="${{p.y}}">${{node}}</text>`);
        parts.push(`<text class="node-side" x="${{p.x}}" y="${{p.y + 50}}">${{side}}</text>`);
      }}

      svg.innerHTML = parts.join("");

      for (const hit of svg.querySelectorAll(".edge-hit")) {{
        hit.addEventListener("click", () => {{
          selectedEdgeKey = hit.dataset.edgeKey;
          render();
        }});
      }}
      for (const circle of svg.querySelectorAll(".node-circle")) {{
        circle.addEventListener("click", () => {{
          const node = Number(circle.dataset.nodeId);
          selectedNode = selectedNode === node ? null : node;
          selectedEdgeKey = null;
          render();
        }});
      }}
    }}

    function render() {{
      const flows = getFilteredFlows();
      const edges = buildEdges(flows);
      const selectedEdge = selectedEdgeKey ? edges.find((edge) => edge.key === selectedEdgeKey) || null : null;

      statViewFlows.textContent = flows.length;
      epochTitle.textContent = showPrefix ? `Epoch 0..${{currentEpoch}}` : `Epoch ${{currentEpoch}}`;
      epochReadout.textContent = showPrefix
        ? `Showing all flows from epoch 0 through epoch ${{currentEpoch}}`
        : `Showing only flows scheduled in epoch ${{currentEpoch}}`;
      const crossCount = flows.filter((flow) => flow.cross_group).length;
      epochSummary.textContent = `visible_edges=${{edges.length}} | cross_chassis_flows=${{crossCount}} | selected_node=${{selectedNode === null ? "none" : selectedNode}}`;

      buildEpochStrip();
      renderGraph(edges);
      renderDetails(selectedEdge, flows);
    }}

    function step(delta) {{
      currentEpoch = Math.max(0, Math.min(Number(epochSlider.max), currentEpoch + delta));
      epochSlider.value = currentEpoch;
      selectedEdgeKey = null;
      render();
    }}

    function togglePlay() {{
      const playBtn = document.getElementById("playBtn");
      if (timer) {{
        clearInterval(timer);
        timer = null;
        playBtn.textContent = "Play";
        return;
      }}
      playBtn.textContent = "Pause";
      timer = setInterval(() => {{
        if (currentEpoch >= Number(epochSlider.max)) {{
          clearInterval(timer);
          timer = null;
          playBtn.textContent = "Play";
          return;
        }}
        step(1);
      }}, 1000);
    }}

    function resetFilters() {{
      sourceFilter.value = "";
      senderFilter.value = "";
      receiverFilter.value = "";
      edgeFilter.value = "all";
      selectedNode = null;
      selectedEdgeKey = null;
      render();
    }}

    epochSlider.addEventListener("input", (e) => {{
      currentEpoch = Number(e.target.value);
      selectedEdgeKey = null;
      render();
    }});
    sourceFilter.addEventListener("change", () => {{ selectedEdgeKey = null; render(); }});
    senderFilter.addEventListener("change", () => {{ selectedEdgeKey = null; render(); }});
    receiverFilter.addEventListener("change", () => {{ selectedEdgeKey = null; render(); }});
    edgeFilter.addEventListener("change", () => {{ selectedEdgeKey = null; render(); }});
    document.getElementById("prevBtn").addEventListener("click", () => step(-1));
    document.getElementById("nextBtn").addEventListener("click", () => step(1));
    document.getElementById("playBtn").addEventListener("click", togglePlay);
    document.getElementById("modeBtn").addEventListener("click", (e) => {{
      showPrefix = !showPrefix;
      e.target.textContent = showPrefix ? "Show Single Epoch" : "Show Prefix 0..epoch";
      selectedEdgeKey = null;
      render();
    }});
    document.getElementById("resetBtn").addEventListener("click", resetFilters);

    render();
  </script>
</body>
</html>
"""


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_name(input_path.stem + "_graph.html")

    with open(input_path, "r", encoding="utf-8") as f:
        schedule_json = json.load(f)

    flows = parse_flows(schedule_json)
    model = build_model(schedule_json, flows, args.split_at)
    html = render_html(model)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Flow graph visualization written to {output_path}")


if __name__ == "__main__":
    main()
