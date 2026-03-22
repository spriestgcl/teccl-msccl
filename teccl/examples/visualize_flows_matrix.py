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
        description="Visualize TECCL 7-Flows as a matrix-oriented self-contained HTML page."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TECCL schedule JSON.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output HTML. Defaults to input path with _matrix.html suffix.",
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


def build_model(schedule_json, flows, split_at):
    nodes = sorted(
        {
            node
            for flow in flows
            for node in (flow["source"], flow["sender"], flow["receiver"])
        }
    )
    if split_at is None and len(nodes) in (15, 16):
        split_at = 8

    epochs_required = int(schedule_json.get("3-Epochs_Required", max(f["epoch"] for f in flows) + 1))
    flows_by_epoch = {epoch: [] for epoch in range(epochs_required)}
    epoch_pair_counts = {epoch: {} for epoch in range(epochs_required)}

    for flow in flows:
        flow["cross_group"] = (
            split_at is not None
            and ((flow["sender"] < split_at) != (flow["receiver"] < split_at))
        )
        flow["switch_path"] = bool(flow["switches"])
        flows_by_epoch.setdefault(flow["epoch"], []).append(flow)

    for epoch, epoch_flows in flows_by_epoch.items():
        epoch_flows.sort(key=lambda f: (f["sender"], f["receiver"], f["source"], f["chunk"]))
        pairs = {}
        for flow in epoch_flows:
            key = f"{flow['sender']}->{flow['receiver']}"
            pairs.setdefault(key, []).append(flow)
        epoch_pair_counts[epoch] = {key: len(value) for key, value in pairs.items()}

    sender_stats = {}
    receiver_stats = {}
    for node in nodes:
        sender_stats[node] = sum(1 for flow in flows if flow["sender"] == node)
        receiver_stats[node] = sum(1 for flow in flows if flow["receiver"] == node)

    return {
        "title": Path(schedule_json.get("InstanceParams", {}).get("schedule_output_file", "")).name
        or "TECCL 7-Flows",
        "nodes": nodes,
        "split_at": split_at,
        "epochs_required": epochs_required,
        "epoch_duration": schedule_json.get("1-Epoch_Duration"),
        "algo_bandwidth": schedule_json.get("5-Algo_Bandwidth"),
        "flow_count": len(flows),
        "cross_group_flow_count": sum(1 for flow in flows if flow["cross_group"]),
        "switch_path_flow_count": sum(1 for flow in flows if flow["switch_path"]),
        "flows_by_epoch": flows_by_epoch,
        "epoch_pair_counts": epoch_pair_counts,
        "sender_stats": sender_stats,
        "receiver_stats": receiver_stats,
    }


def render_html(model):
    data_json = json.dumps(model, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TECCL 7-Flows Matrix Viewer</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffdf9;
      --ink: #1d2430;
      --muted: #667085;
      --line: #d7cfbf;
      --accent: #8f2d1f;
      --accent-2: #235789;
      --chassis-a: #f6eadf;
      --chassis-b: #e7f0f7;
      --cross: #f9d9d3;
      --switch: #ffe2c2;
      --empty: #faf7f1;
      --self: #ece7dc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fffaf2 0, transparent 24%),
        radial-gradient(circle at bottom right, #ece4d8 0, transparent 30%),
        var(--bg);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
    }}
    .app {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 18px;
      padding: 18px;
      min-height: 100vh;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 14px 36px rgba(44, 37, 24, 0.08);
    }}
    .sidebar {{
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }}
    .main {{
      padding: 18px;
      display: grid;
      grid-template-rows: auto auto 1fr auto;
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
      background: rgba(255,255,255,0.7);
    }}
    .stat strong {{
      display: block;
      color: var(--accent);
      font-size: 22px;
      line-height: 1.1;
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
    .epoch-strip {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(56px, 1fr));
      gap: 8px;
    }}
    .epoch-pill {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 8px 6px;
      text-align: center;
      background: white;
      cursor: pointer;
    }}
    .epoch-pill.active {{
      background: var(--accent);
      border-color: var(--accent);
      color: white;
    }}
    .epoch-pill .count {{
      display: block;
      font-size: 18px;
      font-weight: 700;
    }}
    .matrix-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #fff;
    }}
    table.matrix {{
      border-collapse: separate;
      border-spacing: 0;
      width: max-content;
      min-width: 100%;
      table-layout: fixed;
    }}
    .matrix th,
    .matrix td {{
      border-right: 1px solid #e8e1d4;
      border-bottom: 1px solid #e8e1d4;
      vertical-align: top;
    }}
    .matrix thead th {{
      position: sticky;
      top: 0;
      z-index: 3;
      background: #fcfaf6;
    }}
    .matrix tbody th {{
      position: sticky;
      left: 0;
      z-index: 2;
      background: #fcfaf6;
    }}
    .matrix .corner {{
      left: 0;
      z-index: 4;
      min-width: 140px;
      text-align: left;
      padding: 10px;
    }}
    .matrix .header {{
      min-width: 86px;
      width: 86px;
      text-align: center;
      padding: 8px 4px;
      font-size: 13px;
    }}
    .matrix .rowhead {{
      min-width: 140px;
      width: 140px;
      padding: 8px 10px;
      text-align: left;
      font-size: 13px;
      white-space: nowrap;
    }}
    .matrix td {{
      width: 86px;
      min-width: 86px;
      height: 86px;
      padding: 4px;
      background: var(--empty);
    }}
    .matrix td.self {{
      background: var(--self);
    }}
    .matrix td.chassis-a {{
      background: var(--chassis-a);
    }}
    .matrix td.chassis-b {{
      background: var(--chassis-b);
    }}
    .matrix td.cross {{
      background: var(--cross);
    }}
    .matrix td.switch {{
      box-shadow: inset 0 0 0 3px var(--switch);
    }}
    .cell {{
      display: flex;
      flex-direction: column;
      gap: 4px;
      height: 100%;
    }}
    .count-badge {{
      align-self: flex-start;
      border-radius: 999px;
      background: rgba(0,0,0,0.08);
      padding: 1px 7px;
      font-size: 11px;
      font-weight: 700;
    }}
    .mini-list {{
      display: flex;
      flex-direction: column;
      gap: 3px;
      overflow: hidden;
    }}
    .mini-item {{
      border-radius: 8px;
      padding: 3px 5px;
      background: rgba(255,255,255,0.78);
      font-size: 11px;
      line-height: 1.2;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .mini-item.cross {{
      border-left: 4px solid var(--accent);
    }}
    .mini-item.switch {{
      border-left: 4px solid #cc7a00;
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
      width: 14px;
      height: 14px;
      margin-right: 6px;
      vertical-align: -2px;
      border-radius: 4px;
      border: 1px solid rgba(0,0,0,0.08);
      background: var(--legend-color, white);
    }}
    .detail-wrap {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255,255,255,0.85);
      overflow: auto;
      max-height: 320px;
    }}
    table.detail {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .detail th,
    .detail td {{
      padding: 8px 10px;
      border-bottom: 1px solid #e8e1d4;
      text-align: left;
      vertical-align: top;
    }}
    .detail th {{
      position: sticky;
      top: 0;
      background: #fcfaf6;
    }}
    code {{
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
    }}
    @media (max-width: 1100px) {{
      .app {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="panel sidebar">
      <div>
        <h1>7-Flows Matrix</h1>
        <div class="subtle">每个格子就是一个 sender -&gt; receiver。格子里的内容直接列出这个 hop 上承载的 <code>src/chunk</code>。</div>
      </div>
      <div class="stats">
        <div class="stat"><strong id="statNodes"></strong><span>Nodes</span></div>
        <div class="stat"><strong id="statFlows"></strong><span>Total Flows</span></div>
        <div class="stat"><strong id="statEpochs"></strong><span>Epochs</span></div>
        <div class="stat"><strong id="statCurrent"></strong><span>Flows In View</span></div>
      </div>
      <div id="meta" class="subtle"></div>
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
          <button id="modeBtn">Show Prefix 0..epoch</button>
        </div>
      </div>
      <div class="subtle">
        推荐看法：
        <br>1. 先看上面的 epoch 统计条，知道哪个 epoch 最拥挤
        <br>2. 再看矩阵，行是 sender，列是 receiver
        <br>3. 红底块就是跨机箱流
        <br>4. 每个小条目就是一个具体 7-Flow
      </div>
    </aside>
    <main class="panel main">
      <div>
        <div style="font-size:30px;font-weight:700;" id="epochTitle"></div>
        <div class="subtle" id="epochSummary"></div>
      </div>
      <div id="epochStrip" class="epoch-strip"></div>
      <div class="matrix-wrap">
        <table class="matrix">
          <thead id="matrixHead"></thead>
          <tbody id="matrixBody"></tbody>
        </table>
      </div>
      <div>
        <div class="legend">
          <span style="--legend-color:var(--chassis-a);">机箱 0 内部</span>
          <span style="--legend-color:var(--chassis-b);">机箱 1 内部</span>
          <span style="--legend-color:var(--cross);">跨机箱</span>
          <span style="--legend-color:var(--self);">sender = receiver</span>
        </div>
        <div class="detail-wrap" style="margin-top:12px;">
          <table class="detail">
            <thead>
              <tr>
                <th>#</th>
                <th>Epoch</th>
                <th>Sender</th>
                <th>Receiver</th>
                <th>Source</th>
                <th>Chunk</th>
                <th>Switches</th>
                <th>Flow</th>
              </tr>
            </thead>
            <tbody id="detailBody"></tbody>
          </table>
        </div>
      </div>
    </main>
  </div>

  <script>
    const MODEL = {data_json};
    const nodes = MODEL.nodes;
    const epochSlider = document.getElementById("epochSlider");
    const epochReadout = document.getElementById("epochReadout");
    const epochTitle = document.getElementById("epochTitle");
    const epochSummary = document.getElementById("epochSummary");
    const epochStrip = document.getElementById("epochStrip");
    const matrixHead = document.getElementById("matrixHead");
    const matrixBody = document.getElementById("matrixBody");
    const detailBody = document.getElementById("detailBody");
    const statNodes = document.getElementById("statNodes");
    const statFlows = document.getElementById("statFlows");
    const statEpochs = document.getElementById("statEpochs");
    const statCurrent = document.getElementById("statCurrent");
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
    metaParts.push(`switch_path_flows=${{MODEL.switch_path_flow_count}}`);
    if (MODEL.epoch_duration !== null && MODEL.epoch_duration !== undefined) {{
      metaParts.push(`epoch_duration=${{MODEL.epoch_duration}}`);
    }}
    if (MODEL.algo_bandwidth !== null && MODEL.algo_bandwidth !== undefined) {{
      metaParts.push(`algo_bandwidth=${{MODEL.algo_bandwidth}}`);
    }}
    meta.textContent = metaParts.join(" | ");

    function inFirstChassis(node) {{
      if (MODEL.split_at === null || MODEL.split_at === undefined) return true;
      return node < MODEL.split_at;
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

    function pairKey(sender, receiver) {{
      return `${{sender}}->${{receiver}}`;
    }}

    function buildEpochStrip() {{
      const items = [];
      for (let e = 0; e < MODEL.epochs_required; e++) {{
        const count = (MODEL.flows_by_epoch[e] || []).length;
        items.push(`
          <button class="epoch-pill ${{e === currentEpoch ? "active" : ""}}" data-epoch="${{e}}">
            <span>E${{e}}</span>
            <span class="count">${{count}}</span>
          </button>
        `);
      }}
      epochStrip.innerHTML = items.join("");
      for (const btn of epochStrip.querySelectorAll(".epoch-pill")) {{
        btn.addEventListener("click", () => {{
          currentEpoch = Number(btn.dataset.epoch);
          epochSlider.value = currentEpoch;
          render();
        }});
      }}
    }}

    function renderMatrix(flows) {{
      const pairMap = new Map();
      for (const flow of flows) {{
        const key = pairKey(flow.sender, flow.receiver);
        if (!pairMap.has(key)) pairMap.set(key, []);
        pairMap.get(key).push(flow);
      }}

      matrixHead.innerHTML = `
        <tr>
          <th class="corner">sender \\\\ receiver</th>
          ${{
            nodes.map((receiver) => {{
              const side = inFirstChassis(receiver) ? "C0" : "C1";
              return `<th class="header">${{receiver}}<br><span style="color:#7a7a7a;font-weight:400;">${{side}}</span></th>`;
            }}).join("")
          }}
        </tr>
      `;

      const rows = [];
      for (const sender of nodes) {{
        const rowParts = [];
        rowParts.push(`<th class="rowhead">${{sender}}<br><span style="color:#7a7a7a;font-weight:400;">sent=${{MODEL.sender_stats[sender]}}</span></th>`);
        for (const receiver of nodes) {{
          const key = pairKey(sender, receiver);
          const cellFlows = pairMap.get(key) || [];
          const classes = [];
          if (sender === receiver) {{
            classes.push("self");
          }} else if (MODEL.split_at !== null && MODEL.split_at !== undefined) {{
            if (inFirstChassis(sender) && inFirstChassis(receiver)) {{
              classes.push("chassis-a");
            }} else if (!inFirstChassis(sender) && !inFirstChassis(receiver)) {{
              classes.push("chassis-b");
            }} else {{
              classes.push("cross");
            }}
          }}
          if (cellFlows.some((f) => f.switch_path)) {{
            classes.push("switch");
          }}

          let content = "";
          if (cellFlows.length) {{
            const items = cellFlows.map((flow) => {{
              const extraClass = flow.switch_path ? "switch" : (flow.cross_group ? "cross" : "");
              const switchText = flow.switches.length ? ` via:${{flow.switches.join("->")}}` : "";
              return `<div class="mini-item ${{extraClass}}" title="${{flow.label}}">src${{flow.source}} c${{flow.chunk}}${{switchText}}</div>`;
            }}).join("");
            content = `
              <div class="cell">
                <div class="count-badge">${{cellFlows.length}} flow${{cellFlows.length > 1 ? "s" : ""}}</div>
                <div class="mini-list">${{items}}</div>
              </div>
            `;
          }}
          rowParts.push(`<td class="${{classes.join(" ")}}">${{content}}</td>`);
        }}
        rows.push(`<tr>${{rowParts.join("")}}</tr>`);
      }}
      matrixBody.innerHTML = rows.join("");
    }}

    function renderDetails(flows) {{
      detailBody.innerHTML = flows.map((flow, idx) => `
        <tr>
          <td>${{idx + 1}}</td>
          <td>${{flow.epoch}}</td>
          <td>${{flow.sender}}</td>
          <td>${{flow.receiver}}</td>
          <td>${{flow.source}}</td>
          <td>${{flow.chunk}}</td>
          <td>${{flow.switches.join(" -> ")}}</td>
          <td><code>${{flow.label}}</code></td>
        </tr>
      `).join("");
    }}

    function render() {{
      const flows = collectFlows(currentEpoch);
      const crossCount = flows.filter((flow) => flow.cross_group).length;
      const activePairs = new Set(flows.map((flow) => pairKey(flow.sender, flow.receiver))).size;

      statCurrent.textContent = flows.length;
      epochTitle.textContent = showPrefix ? `Epoch 0..${{currentEpoch}}` : `Epoch ${{currentEpoch}}`;
      epochReadout.textContent = showPrefix
        ? `Showing all flows from epoch 0 through epoch ${{currentEpoch}}`
        : `Showing only flows scheduled in epoch ${{currentEpoch}}`;
      epochSummary.textContent = `active sender->receiver pairs = ${{activePairs}} | cross-chassis flows in view = ${{crossCount}}`;

      buildEpochStrip();
      renderMatrix(flows);
      renderDetails(flows);
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
    document.getElementById("modeBtn").addEventListener("click", (e) => {{
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
    output_path = Path(args.output) if args.output else input_path.with_name(input_path.stem + "_matrix.html")

    with open(input_path, "r", encoding="utf-8") as f:
        schedule_json = json.load(f)

    flows = parse_flows(schedule_json)
    model = build_model(schedule_json, flows, args.split_at)
    html = render_html(model)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Flow matrix visualization written to {output_path}")


if __name__ == "__main__":
    main()
