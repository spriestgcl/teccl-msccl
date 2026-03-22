import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

FLOW_RE = re.compile(
    r"Chunk (?P<chunk>\d+) from (?P<source>\d+) traveled over "
    r"(?P<sender>\d+)->(?P<receiver>\d+) in epoch (?P<epoch>\d+)"
)


@dataclass(frozen=True)
class Flow:
    chunk: int
    source: int
    sender: int
    receiver: int
    epoch: int


@dataclass
class Step:
    sends: List[Tuple[int, int, int]]


class SimpleInstance:
    extra_memory = None


class DenseGpuTopology:
    def __init__(self, gpu_count: int, name: str = "NVSwitch16"):
        self.name = name
        self.switches = []
        self.links = [[0] * gpu_count for _ in range(gpu_count)]
        for src in range(gpu_count):
            for dst in range(gpu_count):
                if src != dst:
                    self.links[src][dst] = 1

    def link(self, src: int, dst: int) -> int:
        return self.links[src][dst]


class SimpleAlgorithm:
    def __init__(
        self,
        gpu_count: int,
        chunk_factor: int,
        steps: List[Step],
        topology_name: str = "NVSwitch16",
    ):
        self.steps = steps
        self.name = "Allgather"
        self.instance = SimpleInstance()
        self.topology = DenseGpuTopology(gpu_count, topology_name)
        self.input_map = {}
        self.output_map = {}

        output_addrs = set(range(gpu_count * chunk_factor))
        for rank in range(gpu_count):
            self.input_map[rank] = {
                rank * chunk_factor + chunk for chunk in range(chunk_factor)
            }
            self.output_map[rank] = set(output_addrs)

    def ranks(self):
        return range(len(self.topology.links))

    def is_pipelined(self):
        return False


def _make_nop_step_xml(step_idx: int, dep: Tuple[int, int], algostep: int) -> ET.Element:
    elem = ET.Element("step")
    elem.set("s", str(step_idx))
    elem.set("type", "nop")
    elem.set("algostep", str(algostep))
    elem.set("srcbuf", "i")
    elem.set("srcoff", "-1")
    elem.set("dstbuf", "o")
    elem.set("dstoff", "-1")
    elem.set("cnt", "0")
    elem.set("depid", str(dep[0]))
    elem.set("deps", str(dep[1]))
    elem.set("hasdep", "0")
    return elem


def _reindex_tb_steps(tb: ET.Element) -> None:
    for idx, step in enumerate(tb.findall("step")):
        step.set("s", str(idx))


def _step_dep(step: ET.Element) -> Tuple[int, int]:
    depid = int(step.attrib.get("depid", "-1"))
    deps = int(step.attrib.get("deps", "-1"))
    return depid, deps


def _set_step_dep(step: ET.Element, dep: Tuple[int, int]) -> None:
    step.set("depid", str(dep[0]))
    step.set("deps", str(dep[1]))


def _append_epoch_barriers(xml_text: str) -> str:
    root = ET.fromstring(xml_text)

    for gpu in root.findall("gpu"):
        tbs = gpu.findall("tb")
        if not tbs:
            continue

        rank = int(gpu.attrib["id"])
        tb_by_id = {int(tb.attrib["id"]): tb for tb in tbs}
        next_tb_id = max(tb_by_id) + 1
        anchor_tb = ET.SubElement(
            gpu,
            "tb",
            {
                "id": str(next_tb_id),
                "send": "-1",
                "recv": "-1",
                "chan": "0",
            },
        )

        epoch_to_tbs: Dict[int, List[ET.Element]] = {}
        for tb in tbs:
            epochs = {
                int(step.attrib["algostep"])
                for step in tb.findall("step")
                if step.attrib.get("type") != "nop"
            }
            for epoch in epochs:
                epoch_to_tbs.setdefault(epoch, []).append(tb)

        if not epoch_to_tbs:
            continue

        previous_anchor = None
        anchor_steps = []
        for epoch in sorted(epoch_to_tbs):
            tb_terminals = []

            if previous_anchor is not None:
                for tb in epoch_to_tbs[epoch]:
                    steps = tb.findall("step")
                    first_idx = None
                    for idx, step in enumerate(steps):
                        if int(step.attrib["algostep"]) == epoch:
                            first_idx = idx
                            break
                    if first_idx is None:
                        continue

                    first_step = steps[first_idx]
                    if _step_dep(first_step) == (-1, -1):
                        _set_step_dep(first_step, previous_anchor)
                    else:
                        barrier_step = _make_nop_step_xml(-1, previous_anchor, epoch)
                        tb.insert(first_idx, barrier_step)
                        _reindex_tb_steps(tb)

            for tb in epoch_to_tbs[epoch]:
                steps = [
                    step
                    for step in tb.findall("step")
                    if int(step.attrib["algostep"]) == epoch
                ]
                if steps:
                    last = steps[-1]
                    tb_terminals.append((int(tb.attrib["id"]), int(last.attrib["s"])))

            for dep in tb_terminals:
                anchor_steps.append(_make_nop_step_xml(-1, dep, epoch))

            _reindex_tb_steps(anchor_tb)
            for idx, step in enumerate(anchor_steps):
                step.set("s", str(idx))
            previous_anchor = (next_tb_id, len(anchor_steps) - 1)

        for step in anchor_steps:
            anchor_tb.append(step)

        _reindex_tb_steps(anchor_tb)

        # Mark all operations that are depended on by any step.
        hasdep_targets = set()
        for tb in gpu.findall("tb"):
            for step in tb.findall("step"):
                dep = _step_dep(step)
                if dep != (-1, -1):
                    hasdep_targets.add(dep)

        for tb in gpu.findall("tb"):
            tbid = int(tb.attrib["id"])
            for step in tb.findall("step"):
                key = (tbid, int(step.attrib["s"]))
                step.set("hasdep", "1" if key in hasdep_targets else "0")

    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode")


def _strip_algostep_attributes(xml_text: str) -> str:
    root = ET.fromstring(xml_text)
    for elem in root.iter():
        elem.attrib.pop("algostep", None)
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode")


def parse_flows(schedule_json: Dict) -> List[Flow]:
    raw_flows = []
    min_gpu_id = None
    for flow_str in schedule_json.get("7-Flows", []):
        match = FLOW_RE.match(flow_str)
        if not match:
            raise ValueError(f"Unsupported flow format: {flow_str}")
        raw = {key: int(value) for key, value in match.groupdict().items()}
        raw_flows.append(raw)
        flow_min = min(raw["source"], raw["sender"], raw["receiver"])
        min_gpu_id = flow_min if min_gpu_id is None else min(min_gpu_id, flow_min)

    if not raw_flows:
        raise ValueError("Input schedule JSON does not contain 7-Flows.")

    if min_gpu_id == 0:
        gpu_id_offset = 0
    elif min_gpu_id == 1:
        gpu_id_offset = 1
    else:
        raise ValueError(
            f"Unsupported GPU numbering in schedule JSON. Minimum GPU id was {min_gpu_id}, "
            "expected either 0-based or 1-based numbering."
        )

    flows = []
    for raw in raw_flows:
        flows.append(
            Flow(
                chunk=raw["chunk"],
                source=raw["source"] - gpu_id_offset,
                sender=raw["sender"] - gpu_id_offset,
                receiver=raw["receiver"] - gpu_id_offset,
                epoch=raw["epoch"],
            )
        )
    return flows


def build_steps(
    flows: List[Flow],
    gpu_count: int,
    chunk_factor: int,
    total_epochs: int,
) -> List[Step]:
    flows_by_epoch: Dict[int, List[Flow]] = {}
    for flow in flows:
        flows_by_epoch.setdefault(flow.epoch, []).append(flow)

    # producer[(holder_rank, source_rank, chunk_id)] exists iff the holder owns the chunk
    producers = {
        (rank, rank, chunk): None
        for rank in range(gpu_count)
        for chunk in range(chunk_factor)
    }

    steps = []
    for epoch in range(total_epochs):
        epoch_flows = flows_by_epoch.get(epoch, [])
        epoch_sends = []
        new_producers = {}

        # Sorting makes the lowering deterministic.
        epoch_flows.sort(key=lambda f: (f.sender, f.receiver, f.source, f.chunk))
        for flow in epoch_flows:
            producer_key = (flow.sender, flow.source, flow.chunk)
            if producer_key not in producers:
                raise ValueError(
                    f"GPU {flow.sender + 1} sends chunk {flow.chunk} from GPU "
                    f"{flow.source + 1} in epoch {flow.epoch} before owning it."
                )

            addr = flow.source * chunk_factor + flow.chunk
            epoch_sends.append((addr, flow.sender, flow.receiver))
            new_producers[(flow.receiver, flow.source, flow.chunk)] = producer_key

        producers.update(new_producers)
        steps.append(Step(epoch_sends))

    return steps


def build_algorithm(schedule_json: Dict) -> SimpleAlgorithm:
    flows = parse_flows(schedule_json)
    gpu_ids = sorted(
        {
            gpu
            for flow in flows
            for gpu in (flow.source, flow.sender, flow.receiver)
        }
    )

    if gpu_ids != list(range(len(gpu_ids))):
        raise ValueError(
            "Expected dense GPU ids after zero-basing the schedule."
        )

    gpu_count = len(gpu_ids)
    chunk_factor = max(flow.chunk for flow in flows) + 1
    total_epochs = max(
        int(schedule_json.get("3-Epochs_Required", 0)),
        max(flow.epoch for flow in flows) + 1,
    )
    steps = build_steps(flows, gpu_count, chunk_factor, total_epochs)
    return SimpleAlgorithm(gpu_count, chunk_factor, steps)


def main():
    from ncclize import ChannelPolicy, ncclize

    parser = argparse.ArgumentParser(
        description="Convert a TECCL schedule JSON into an NCCL/MSCCL XML through ncclize."
    )
    parser.add_argument("-i", "--input", required=True, help="Path to TECCL schedule JSON.")
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output XML. Defaults to input path with .xml suffix.",
    )
    parser.add_argument(
        "--msccl-output",
        help=(
            "Optional second XML path for a stricter MSCCL-style file without "
            "the extra algostep attribute."
        ),
    )
    parser.add_argument(
        "--channel-policy",
        choices=[policy.value for policy in ChannelPolicy],
        default=ChannelPolicy.One.value,
        help="Channel allocation policy to use inside ncclize.",
    )
    parser.add_argument(
        "--new-format",
        action="store_true",
        help="Emit ncclize's newer <op> XML instead of the older <step> format.",
    )
    parser.add_argument(
        "--no-merge-contiguous",
        action="store_true",
        help="Disable contiguous interval coalescing inside ncclize.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print ncclize logging.",
    )
    parser.add_argument(
        "--epoch-barriers",
        action="store_true",
        help="Append per-GPU epoch barrier anchors after ncclize lowering.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_suffix(".xml")

    with open(input_path, "r", encoding="utf-8") as f:
        schedule_json = json.load(f)

    algorithm = build_algorithm(schedule_json)
    xml_text = ncclize(
        algorithm,
        channel_policy=ChannelPolicy(args.channel_policy),
        pretty_print=True,
        old_format=not args.new_format,
        use_scratch=False,
        merge_contiguous=not args.no_merge_contiguous,
        instances=1,
        logging=args.verbose,
        epoch_tb_grouping=False,
        include_algostep=True,
    )

    if args.epoch_barriers:
        xml_text = _append_epoch_barriers(xml_text)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_text)

    print(f"XML written to {output_path}")

    if args.msccl_output:
        msccl_output_path = Path(args.msccl_output)
        msccl_output_path.parent.mkdir(parents=True, exist_ok=True)
        strict_xml_text = _strip_algostep_attributes(xml_text)
        with open(msccl_output_path, "w", encoding="utf-8") as f:
            f.write(strict_xml_text)
        print(f"MSCCL-style XML written to {msccl_output_path}")


if __name__ == "__main__":
    main()
