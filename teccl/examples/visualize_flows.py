import argparse
import json
import math
import re
from pathlib import Path


FLOW_RE = re.compile(
    r"Chunk (?P<chunk>\d+) from (?P<source>\d+) traveled over "
    r"(?P<sender>\d+)->(?P<receiver>\d+) in epoch (?P<epoch>\d+)"
    r"(?: via switches (?P<switches>.*))?"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize TECCL 7-Flows as a self-contained HTML page."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TECCL schedule JSON.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output HTML. Defaults to input path with _flows.html suffix.",
    )
    parser.add_argument(
        "--layout",
        choices=["auto", "two-chassis", "circle"],
        default="auto",
        help="Node layout to use in the visualization.",
    )
    parser.add_argument(
        "--split-at",
        type=int,
        default=None,
        help="Split point for two-chassis layout. Example: 8 for nodes 0-7 and 8-15.",
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


def choose_layout(nodes, requested_layout, split_at):
    if requested_layout != "auto":
        return requested_layout, split_at
    node_count = len(nodes)
    if split_at is not None:
        return "two-chassis", split_at
    if node_count in (16, 15):
        # Common two-group cases in this repo.
        return "two-chassis", 8
    return "circle", split_at


def compute_positions(nodes, layout, split_at):
    width = 1200
    height = 760
    positions = {}
    node_count = len(nodes)

    if layout == "two-chassis":
        if split_at is None:
            split_at = max(1, node_count // 2)
        left_nodes = [n for n in nodes if n < split_at]
        right_nodes = [n for n in nodes if n >= split_at]
        groups = [
            (left_nodes, 260, 380, 140),
            (right_nodes, 940, 380, 140),
        ]
        for group_nodes, cx, cy, radius in groups:
            if not group_nodes:
                continue
            step = (2 * math.pi) / max(1, len(group_nodes))
            for idx, node in enumerate(group_nodes):
                angle = -math.pi / 2 + idx * step
                positions[node] = {
                    "x": round(cx + radius * math.cos(angle), 2),
                    "y": round(cy + radius * math.sin(angle), 2),
                }
    else:
        cx, cy, radius = 600, 380, 280
        step = (2 * math.pi) / max(1, node_count)
        for idx, node in enumerate(nodes):
            angle = -math.pi / 2 + idx * step
            positions[node] = {
                "x": round(cx + radius * math.cos(angle), 2),
                "y": round(cy + radius * math.sin(angle), 2),
            }

    return {"width": width, "height": height, "nodes": positions}


def build_model(schedule_json, flows, layout, split_at):
    nodes = sorted(
        {
            node
            for flow in flows
            for node in (flow["source"], flow["sender"], flow["receiver"])
        }
    )
    layout, split_at = choose_layout(nodes, layout, split_at)
    positions = compute_positions(nodes, layout, split_at)
    epochs_required = int(schedule_json.get("3-Epochs_Required", max(f["epoch"] for f in flows) + 1))
    flows_by_epoch = {epoch: [] for epoch in range(epochs_required)}
    for flow in flows:
        flow["cross_group"] = (
            split_at is not None
            and ((flow["sender"] < split_at) != (flow["receiver"] < split_at))
        )
        flows_by_epoch.setdefault(flow["epoch"], []).append(flow)
    for epoch in flows_by_epoch:
        flows_by_epoch[epoch].sort(key=lambda f: (f["sender"], f["receiver"], f["source"], f["chunk"]))

    return {
        "title": Path(schedule_json.get("InstanceParams", {}).get("schedule_output_file", "")).name
        or "TECCL 7-Flows",
        "epochs_required": epochs_required,
        "nodes": nodes,
        "layout": layout,
        "split_at": split_at,
        "positions": positions,
        "flows_by_epoch": flows_by_epoch,
        "flow_count": len(flows),
        "cross_group_flow_count": sum(1 for flow in flows if flow["cross_group"]),
        "epoch_duration": schedule_json.get("1-Epoch_Duration"),
        "algo_bandwidth": schedule_json.get("5-Algo_Bandwidth"),
    }


def render_html(model):
    data_json = json.dumps(model, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>TECCL 7-Flows Viewer</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffaf2;
      --ink: #1e2430;
      --muted: #6b7280;
      --accent: #a33b20;
      --accent-soft: #e9b7a7;
      --line: #d5c6ae;
      --node: #234b6b;
      --node-text: #ffffff;
      --edge: #3f6ea0;
      --edge-dim: #cfbfaa;
      --edge-switch: #bf5a36;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff8ec 0, transparent 28%),
        radial-gradient(circle at bottom right, #efe2cf 0, transparent 35%),
        var(--bg);
    }}
    .app {{
      display: grid;
      grid-template-columns: 340px 1fr;
      min-height: 100vh;
      gap: 18px;
      padding: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 18px 40px rgba(56, 44, 28, 0.10);
    }}
    .sidebar {{
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 18px;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      line-height: 1.1;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 14px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .stat {{
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255,255,255,0.6);
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
    .button-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    button {{
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      padding: 8px 12px;
      border-radius: 999px;
      cursor: pointer;
      font-weight: 600;
    }}
    button.primary {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }}
    input[type="range"] {{
      width: 100%;
      accent-color: var(--accent);
    }}
    .viewer {{
      padding: 18px;
      display: grid;
      grid-template-rows: auto 1fr auto;
      gap: 16px;
    }}
    .viewer-head {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      padding: 4px 8px 0;
    }}
    .epoch-title {{
      font-size: 30px;
      font-weight: 700;
    }}
    .legend {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 13px;
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 16px;
      height: 3px;
      margin-right: 6px;
      vertical-align: middle;
      border-radius: 999px;
      background: var(--legend-color, var(--edge));
    }}
    .legend .switch::before {{ background: var(--edge-switch); }}
    .canvas-wrap {{
      position: relative;
      min-height: 0;
      overflow: hidden;
      border-radius: 16px;
      border: 1px solid var(--line);
      background:
        linear-gradient(180deg, rgba(255,255,255,0.92), rgba(245,236,222,0.96));
    }}
    svg {{
      width: 100%;
      height: 100%;
      min-height: 620px;
      display: block;
    }}
    .table-wrap {{
      max-height: 240px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.65);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid #eadfce;
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #fdf7ef;
    }}
    .node-label {{
      fill: var(--node-text);
      font-size: 14px;
      font-weight: 700;
      text-anchor: middle;
      dominant-baseline: middle;
      pointer-events: none;
    }}
    .node-circle {{
      fill: var(--node);
      stroke: white;
      stroke-width: 3;
    }}
    .chassis-label {{
      font-size: 20px;
      fill: #a78a66;
      font-weight: 700;
      text-anchor: middle;
    }}
    @media (max-width: 1100px) {{
      .app {{ grid-template-columns: 1fr; }}
      .viewer {{ min-height: 70vh; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="panel sidebar">
      <div>
        <h1>TECCL 7-Flows</h1>
        <div class="subtle">Interactive epoch-by-epoch schedule viewer</div>
      </div>
      <div class="stats">
        <div class="stat"><strong id="statNodes"></strong><span>Nodes</span></div>
        <div class="stat"><strong id="statFlows"></strong><span>Total Flows</span></div>
        <div class="stat"><strong id="statEpochs"></strong><span>Epochs</span></div>
        <div class="stat"><strong id="statEpochFlowCount"></strong><span>Flows In View</span></div>
      </div>
      <div class="subtle" id="meta"></div>
      <div class="controls">
        <label for="epochSlider"><strong>Epoch</strong></label>
        <input id="epochSlider" type="range" min="0" max="0" value="0">
        <div id="epochReadout" class="subtle"></div>
        <div class="button-row">
          <button id="prevBtn">Prev</button>
          <button id="playBtn" class="primary">Play</button>
          <button id="nextBtn">Next</button>
        </div>
        <div class="button-row">
          <button id="toggleAllBtn">Show Prefix 0..epoch</button>
        </div>
      </div>
      <div class="subtle">
        Edge label format: <code>sender-&gt;receiver | src source | chunk id</code>.<br>
        Red edges are cross-chassis flows. Orange edges indicate the flow string included <code>via switches ...</code>.
      </div>
    </aside>
    <main class="panel viewer">
      <div class="viewer-head">
        <div class="epoch-title" id="epochTitle"></div>
        <div class="legend">
          <span class="normal">Intra-chassis edge</span>
          <span class="normal" style="--legend-color:#8f2d1f;">Cross-chassis edge</span>
          <span class="switch">Flow string contains switches</span>
        </div>
      </div>
      <div class="canvas-wrap">
        <svg id="canvas" viewBox="0 0 1200 760"></svg>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Sender</th>
              <th>Receiver</th>
              <th>Source</th>
              <th>Chunk</th>
              <th>Switches</th>
              <th>Flow</th>
            </tr>
          </thead>
          <tbody id="flowTableBody"></tbody>
        </table>
      </div>
    </main>
  </div>

  <script>
    const MODEL = {data_json};
    const svg = document.getElementById("canvas");
    const epochSlider = document.getElementById("epochSlider");
    const epochTitle = document.getElementById("epochTitle");
    const epochReadout = document.getElementById("epochReadout");
    const flowTableBody = document.getElementById("flowTableBody");
    const statNodes = document.getElementById("statNodes");
    const statFlows = document.getElementById("statFlows");
    const statEpochs = document.getElementById("statEpochs");
    const statEpochFlowCount = document.getElementById("statEpochFlowCount");
    const meta = document.getElementById("meta");

    let currentEpoch = 0;
    let showPrefix = false;
    let timer = null;

    epochSlider.max = Math.max(0, MODEL.epochs_required - 1);
    statNodes.textContent = MODEL.nodes.length;
    statFlows.textContent = MODEL.flow_count;
    statEpochs.textContent = MODEL.epochs_required;

    const metaParts = [];
    metaParts.push(`cross_chassis_flows=${{MODEL.cross_group_flow_count}}`);
    if (MODEL.epoch_duration !== null && MODEL.epoch_duration !== undefined) {{
      metaParts.push(`epoch_duration=${{MODEL.epoch_duration}}`);
    }}
    if (MODEL.algo_bandwidth !== null && MODEL.algo_bandwidth !== undefined) {{
      metaParts.push(`algo_bandwidth=${{MODEL.algo_bandwidth}}`);
    }}
    meta.textContent = metaParts.join(" | ");

    function lineAttrs(flow) {{
      const p1 = MODEL.positions.nodes[flow.sender];
      const p2 = MODEL.positions.nodes[flow.receiver];
      const dx = p2.x - p1.x;
      const dy = p2.y - p1.y;
      const len = Math.sqrt(dx * dx + dy * dy) || 1;
      const ux = dx / len;
      const uy = dy / len;
      const startX = p1.x + ux * 24;
      const startY = p1.y + uy * 24;
      const endX = p2.x - ux * 24;
      const endY = p2.y - uy * 24;
      return {{ startX, startY, endX, endY, midX: (startX + endX) / 2, midY: (startY + endY) / 2 }};
    }}

    function collectFlows(epoch) {{
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

    function render() {{
      const flows = collectFlows(currentEpoch);
      statEpochFlowCount.textContent = flows.length;
      epochTitle.textContent = showPrefix ? `Epoch 0..${{currentEpoch}}` : `Epoch ${{currentEpoch}}`;
      epochReadout.textContent = showPrefix
        ? `Showing all flows from epoch 0 through epoch ${{currentEpoch}}`
        : `Showing only flows scheduled in epoch ${{currentEpoch}}`;

      const parts = [];
      parts.push(`<defs>
        <marker id="arrow-blue" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="#3f6ea0"></path>
        </marker>
        <marker id="arrow-orange" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="#bf5a36"></path>
        </marker>
      </defs>`);

      if (MODEL.layout === "two-chassis") {{
        parts.push(`<text class="chassis-label" x="260" y="78">Chassis 0</text>`);
        parts.push(`<text class="chassis-label" x="940" y="78">Chassis 1</text>`);
        if (MODEL.split_at !== null && MODEL.split_at !== undefined) {{
          parts.push(`<text x="260" y="104" font-size="13" text-anchor="middle" fill="#8e7757">GPU 0 .. ${{MODEL.split_at - 1}}</text>`);
          parts.push(`<text x="940" y="104" font-size="13" text-anchor="middle" fill="#8e7757">GPU ${{MODEL.split_at}} .. ${{MODEL.nodes[MODEL.nodes.length - 1]}}</text>`);
        }}
        parts.push(`<rect x="90" y="120" width="340" height="500" rx="20" fill="none" stroke="#d9c7af" stroke-width="2" stroke-dasharray="6 6"></rect>`);
        parts.push(`<rect x="770" y="120" width="340" height="500" rx="20" fill="none" stroke="#d9c7af" stroke-width="2" stroke-dasharray="6 6"></rect>`);
      }}

      for (const flow of flows) {{
        const a = lineAttrs(flow);
        const color = flow.switches.length
          ? "#bf5a36"
          : (flow.cross_group ? "#8f2d1f" : "#3f6ea0");
        const marker = flow.switches.length || flow.cross_group ? "url(#arrow-orange)" : "url(#arrow-blue)";
        const width = showPrefix ? 2.4 : 3.2;
        parts.push(`<line x1="${{a.startX}}" y1="${{a.startY}}" x2="${{a.endX}}" y2="${{a.endY}}" stroke="${{color}}" stroke-width="${{width}}" stroke-linecap="round" marker-end="${{marker}}" opacity="0.92"></line>`);
        parts.push(`<text x="${{a.midX}}" y="${{a.midY - 7}}" font-size="12" text-anchor="middle" fill="${{color}}" font-weight="700">${{flow.sender}}-&gt;${{flow.receiver}}</text>`);
        parts.push(`<text x="${{a.midX}}" y="${{a.midY + 9}}" font-size="11" text-anchor="middle" fill="#7a5c47">src ${{flow.source}}, c${{flow.chunk}}</text>`);
      }}

      for (const node of MODEL.nodes) {{
        const p = MODEL.positions.nodes[node];
        parts.push(`<circle class="node-circle" cx="${{p.x}}" cy="${{p.y}}" r="22"></circle>`);
        parts.push(`<text class="node-label" x="${{p.x}}" y="${{p.y}}">${{node}}</text>`);
      }}

      svg.innerHTML = parts.join("");

      flowTableBody.innerHTML = flows.map((flow, idx) => `
        <tr>
          <td>${{idx + 1}}</td>
          <td>${{flow.sender}}</td>
          <td>${{flow.receiver}}</td>
          <td>${{flow.source}}</td>
          <td>${{flow.chunk}}</td>
          <td>${{flow.switches.join(" -> ")}}</td>
          <td><code>${{flow.label}}</code></td>
        </tr>
      `).join("");
    }}

    function step(delta) {{
      currentEpoch = Math.max(0, Math.min(Number(epochSlider.max), currentEpoch + delta));
      epochSlider.value = currentEpoch;
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

    epochSlider.addEventListener("input", (e) => {{
      currentEpoch = Number(e.target.value);
      render();
    }});
    document.getElementById("prevBtn").addEventListener("click", () => step(-1));
    document.getElementById("nextBtn").addEventListener("click", () => step(1));
    document.getElementById("playBtn").addEventListener("click", togglePlay);
    document.getElementById("toggleAllBtn").addEventListener("click", (e) => {{
      showPrefix = !showPrefix;
      e.target.textContent = showPrefix ? "Show Single Epoch" : "Show Prefix 0..epoch";
      render();
    }});

    render();
  </script>
</body>
</html>
"""


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_name(input_path.stem + "_flows.html")

    with open(input_path, "r", encoding="utf-8") as f:
        schedule_json = json.load(f)

    flows = parse_flows(schedule_json)
    model = build_model(schedule_json, flows, args.layout, args.split_at)
    html = render_html(model)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Flow visualization written to {output_path}")


if __name__ == "__main__":
    main()
