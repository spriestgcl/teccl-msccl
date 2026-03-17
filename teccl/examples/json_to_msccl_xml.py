import argparse
import json
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


FLOW_RE = re.compile(
    r"Chunk (?P<chunk>\d+) from (?P<source>\d+) traveled over "
    r"(?P<sender>\d+)->(?P<receiver>\d+) in epoch (?P<epoch>\d+)"
)


def indent_xml(elem, level=0):
    indent = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "  "
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    elif level and (not elem.tail or not elem.tail.strip()):
        elem.tail = indent


def parse_flows(flow_strings):
    flows = []
    for flow in flow_strings:
        match = FLOW_RE.match(flow)
        if not match:
            raise ValueError(f"Unsupported flow format: {flow}")
        flows.append({k: int(v) for k, v in match.groupdict().items()})
    return flows


def build_msccl_xml(schedule_json):
    flow_strings = schedule_json.get("7-Flows", [])
    if not flow_strings:
        raise ValueError("Input JSON does not contain 7-Flows.")

    flows = parse_flows(flow_strings)

    gpu_ids = set()
    for flow in flows:
        gpu_ids.add(flow["source"])
        gpu_ids.add(flow["sender"])
        gpu_ids.add(flow["receiver"])

    if gpu_ids != {1, 2, 3, 4}:
        raise NotImplementedError(
            "This converter currently supports only the A800_4GPU numbering scheme with GPUs 1..4."
        )

    per_gpu_peer_steps = defaultdict(lambda: defaultdict(list))
    for flow in sorted(flows, key=lambda item: (item["epoch"], item["sender"], item["receiver"], item["source"])):
        src_rank = flow["source"] - 1
        send_rank = flow["sender"] - 1
        recv_rank = flow["receiver"] - 1
        epoch = flow["epoch"]

        step_desc = {
            "epoch": epoch,
            "offset": src_rank,
            "count": 1,
        }
        per_gpu_peer_steps[send_rank][("send", recv_rank)].append(step_desc)
        per_gpu_peer_steps[recv_rank][("recv", send_rank)].append(step_desc)

    algo = ET.Element(
        "algo",
        {
            "name": "teccl_a800_4gpu_allgather",
            "proto": "Simple",
            "nchannels": "1",
            "nchunksperloop": "4",
            "ngpus": "4",
            "coll": "allgather",
            "inplace": "1",
            "outofplace": "0",
            "minBytes": "1",
            "maxBytes": str(1 << 60),
        },
    )

    for rank in range(4):
        gpu = ET.SubElement(
            algo,
            "gpu",
            {
                "id": str(rank),
                "i_chunks": "1",
                "o_chunks": "4",
                "s_chunks": "0",
            },
        )

        tb_id = 0
        for (direction, peer), steps in sorted(per_gpu_peer_steps[rank].items(), key=lambda item: (item[0][0], item[0][1])):
            if direction == "send":
                tb = ET.SubElement(
                    gpu,
                    "tb",
                    {
                        "id": str(tb_id),
                        "send": str(peer),
                        "recv": "-1",
                        "chan": "0",
                    },
                )
                step_type = "s"
            else:
                tb = ET.SubElement(
                    gpu,
                    "tb",
                    {
                        "id": str(tb_id),
                        "send": "-1",
                        "recv": str(peer),
                        "chan": "0",
                    },
                )
                step_type = "r"

            for idx, step in enumerate(sorted(steps, key=lambda item: item["epoch"])):
                ET.SubElement(
                    tb,
                    "step",
                    {
                        "s": str(idx),
                        "type": step_type,
                        "srcbuf": "o",
                        "srcoff": str(step["offset"]),
                        "dstbuf": "o",
                        "dstoff": str(step["offset"]),
                        "cnt": str(step["count"]),
                        "depid": "-1",
                        "deps": "-1",
                        "hasdep": "0",
                    },
                )
            tb_id += 1

    indent_xml(algo)
    return algo


def main():
    parser = argparse.ArgumentParser(
        description="Convert a TE-CCL schedule JSON into an MSCCL XML for A800_4GPU AllGather."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TE-CCL output JSON.")
    parser.add_argument("-o", "--output", help="Path to output XML. Defaults to input path with .xml suffix.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_suffix(".xml")

    with open(input_path, "r") as f:
        schedule_json = json.load(f)

    algo = build_msccl_xml(schedule_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(algo).write(output_path, encoding="utf-8", xml_declaration=False)
    print(f"MSCCL XML written to {output_path}")


if __name__ == "__main__":
    main()
