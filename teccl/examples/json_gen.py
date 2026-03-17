from itertools import product
import json
import pathlib
from pathlib import Path

BASE_PATH = pathlib.Path(r"./experiments/")
SCHEDULE_PATH = pathlib.Path(r"./experiments/output/")
SAMPLE_INPUTS_PATH = pathlib.Path(r"./sample_inputs/")

KB = 1 / (1024 * 1024)
MB = 1 / 1024
GB = 1

OUTPUT_TOTAL_TRANSMISSION_SIZES = [
    (1 * KB, "1KB"),
    (4 * KB, "4KB"),
    (16 * KB, "16KB"),
    (64 * KB, "64KB"),
    (256 * KB, "256KB"),
    (1 * MB, "1MB"),
    (4 * MB, "4MB"),
    (16 * MB, "16MB"),
    (64 * MB, "64MB"),
    (256 * MB, "256MB"),
    (1 * GB, "1GB")
]

DGX2_2_CHASSIS_ALLTOALL_INPUT_EPOCHS = {
    1 * KB: 920,
    4 * KB: 660,
    16 * KB: 825,
    64 * KB: 560,
    256 * KB: 440,
    1 * MB: 340,
    4 * MB: 400,
    16 * MB: 400,
    64 * MB: 400,
    256 * MB: 400,
    1 * GB: 400
}

def generate_topology_helper(
        sample_json_path: pathlib.Path,
        output_dir: pathlib.Path,
        schedule_output_json_path: pathlib.Path,
        nodes_in_chassis: int,
        chassis: int,
        collective: int,
        epoch_type: int,
        output_tts: float,
        early_stop: bool = False,
        input_epochs_dict: dict = {}):
    with open(sample_json_path, 'r') as f:
        sample_json = json.load(f)
    sample_json["TopologyParams"]["chassis"] = int(chassis)
    devices = chassis * nodes_in_chassis
    chunk_size = output_tts / devices
    sample_json["TopologyParams"]["chunk_size"] = chunk_size

    sample_json["InstanceParams"]["collective"] = int(collective)
    sample_json["InstanceParams"]["epoch_type"] = int(epoch_type)

    # Use solution_method = OneShot for AlltoAll
    if collective == 2:
        sample_json["InstanceParams"]["solution_method"] = 1

    # Add epoch_multiplier
    # Fast link:
    # 4KB: 4, 1KB: 4
    # Slow link:
    # 1KB: 2
    sample_json["InstanceParams"]["epoch_multiplier"] = 1
    if epoch_type == 1:
        if output_tts <= 4e-6:
            sample_json["InstanceParams"]["epoch_multiplier"] = 4
    if epoch_type == 2:
        if output_tts <= 1e-6:
            sample_json["InstanceParams"]["epoch_multiplier"] = 2

    if early_stop:
        sample_json["GurobiParams"]["mip_gap"] = 0.3
        sample_json["InstanceParams"]["solution_method"] = 1
    
    if len(input_epochs_dict) > 0:
        sample_json["InstanceParams"]["num_epochs"] = input_epochs_dict[output_tts]

    sample_json["InstanceParams"]["schedule_output_file"] = str(
        schedule_output_json_path.absolute())
    with open(output_dir, 'w') as wf:
        json.dump(sample_json, wf, indent=2)


def generate_ndv2():
    chassis = [2, 4]
    collective = [1, 2]
    epoch_type = [1, 2]
    # Replace with absolute path
    base_path = BASE_PATH / pathlib.Path("NDv2")
    schedule_path = SCHEDULE_PATH / pathlib.Path(r"NDv2_output")
    for c, col, et in product(chassis, collective, epoch_type):
        chassis_folder = base_path / f"{c}_chassis"
        chassis_output_folder = schedule_path / f"{c}_chassis"
        chassis_folder.mkdir(parents=True, exist_ok=True)
        chassis_output_folder.mkdir(parents=True, exist_ok=True)
        collective_type = "AllGather" if col == 1 else "AlltoAll"
        collective_folder = chassis_folder / collective_type
        collective_output_folder = chassis_output_folder / collective_type
        collective_folder.mkdir(parents=True, exist_ok=True)
        collective_output_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_str = "Fast" if et == 1 else "Slow"
        epoch_type_folder = collective_folder / epoch_type_str
        epoch_type_output_folder = collective_output_folder / epoch_type_str
        epoch_type_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_output_folder.mkdir(parents=True, exist_ok=True)

        # Add Early Stop configs to Fast epoch type
        if et == 1 and col == 1:
            es_folder = collective_folder / "Fast_Early_Stop"
            es_output_folder = collective_output_folder / "Fast_Early_Stop"
            es_folder.mkdir(parents=True, exist_ok=True)
            es_output_folder.mkdir(parents=True, exist_ok=True)

        for tts, name in OUTPUT_TOTAL_TRANSMISSION_SIZES:
            sample_json_path = SAMPLE_INPUTS_PATH / "ndv2_sample.json"
            output_json_path = epoch_type_folder / f"{name}.json"
            schedule_output_json_path = epoch_type_output_folder / \
                f"{name}.json"
            generate_topology_helper(
                sample_json_path, output_json_path, schedule_output_json_path, 8, c, col, et, tts)
            
            # Add Early Stop configs to Fast epoch type
            if et == 1 and col == 1:
                output_json_path = es_folder / Path(f"{name}.json")
                schedule_output_json_path = es_output_folder / Path(f"{name}.json")
                generate_topology_helper(
                    sample_json_path, output_json_path, schedule_output_json_path, 8, c, col, et, tts, early_stop=True)


def generate_dgx2():
    chassis = [2]
    collective = [1, 2]
    epoch_type = [1, 2]
    # Replace with absolute path
    base_path = BASE_PATH / pathlib.Path("DGX2")
    schedule_path = SCHEDULE_PATH / pathlib.Path(r"DGX2_output")
    for c, col, et in product(chassis, collective, epoch_type):
        chassis_folder = base_path / f"{c}_chassis"
        chassis_output_folder = schedule_path / f"{c}_chassis"
        chassis_folder.mkdir(parents=True, exist_ok=True)
        chassis_output_folder.mkdir(parents=True, exist_ok=True)
        collective_type = "AllGather" if col == 1 else "AlltoAll"
        collective_folder = chassis_folder / collective_type
        collective_output_folder = chassis_output_folder / collective_type
        collective_folder.mkdir(parents=True, exist_ok=True)
        collective_output_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_str = "Fast" if et == 1 else "Slow"
        epoch_type_folder = collective_folder / epoch_type_str
        epoch_type_output_folder = collective_output_folder / epoch_type_str
        epoch_type_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_output_folder.mkdir(parents=True, exist_ok=True)

        # Add Early Stop configs for AllGather, Fast epoch type
        if et == 1 and col == 1:
            es_folder = collective_folder / "Fast_Early_Stop"
            es_output_folder = collective_output_folder / "Fast_Early_Stop"
            es_folder.mkdir(parents=True, exist_ok=True)
            es_output_folder.mkdir(parents=True, exist_ok=True)

        for tts, name in OUTPUT_TOTAL_TRANSMISSION_SIZES:
            sample_json_path = SAMPLE_INPUTS_PATH / "dgx2_sample.json"
            output_json_path = epoch_type_folder / f"{name}.json"
            schedule_output_json_path = epoch_type_output_folder / \
                f"{name}.json"
            generate_topology_helper(
                sample_json_path, output_json_path, schedule_output_json_path, 16, c, col, et, tts, input_epochs_dict=DGX2_2_CHASSIS_ALLTOALL_INPUT_EPOCHS)
            
            # Add Early Stop configs to Fast epoch type
            if et == 1 and col == 1:
                output_json_path = es_folder / Path(f"{name}.json")
                schedule_output_json_path = es_output_folder / Path(f"{name}.json")
                generate_topology_helper(
                    sample_json_path, output_json_path, schedule_output_json_path, 8, c, col, et, tts, early_stop=True)


def generate_amd():
    chassis = [2]
    collective = [1]
    epoch_type = [1]
    base_path = BASE_PATH / pathlib.Path(r"AMD")
    schedule_path = SCHEDULE_PATH / pathlib.Path(r"AMD_output")
    for c, col, et in product(chassis, collective, epoch_type):
        chassis_folder = base_path / f"{c}_chassis"
        chassis_output_folder = schedule_path / f"{c}_chassis"
        chassis_folder.mkdir(parents=True, exist_ok=True)
        chassis_output_folder.mkdir(parents=True, exist_ok=True)
        collective_type = "AllGather" if col == 1 else "AlltoAll"
        collective_folder = chassis_folder / collective_type
        collective_output_folder = chassis_output_folder / collective_type
        collective_folder.mkdir(parents=True, exist_ok=True)
        collective_output_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_str = "Fast" if et == 1 else "Slow"
        epoch_type_folder = collective_folder / epoch_type_str
        epoch_type_output_folder = collective_output_folder / epoch_type_str
        epoch_type_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_output_folder.mkdir(parents=True, exist_ok=True)
        for tts, name in OUTPUT_TOTAL_TRANSMISSION_SIZES:
            sample_json_path = SAMPLE_INPUTS_PATH / "amd_sample.json"
            output_json_path = epoch_type_folder / f"{name}.json"
            schedule_output_json_path = epoch_type_output_folder / \
                f"{name}.json"
            generate_topology_helper(
                sample_json_path, output_json_path, schedule_output_json_path, 16, c, col, et, tts)


def generate_a800_4gpu():
    chassis = [1]
    collective = [1, 2]
    epoch_type = [1, 2]
    base_path = BASE_PATH / pathlib.Path("A800_4GPU")
    schedule_path = SCHEDULE_PATH / pathlib.Path("A800_4GPU_output")
    for c, col, et in product(chassis, collective, epoch_type):
        chassis_folder = base_path / f"{c}_chassis"
        chassis_output_folder = schedule_path / f"{c}_chassis"
        chassis_folder.mkdir(parents=True, exist_ok=True)
        chassis_output_folder.mkdir(parents=True, exist_ok=True)
        collective_type = "AllGather" if col == 1 else "AlltoAll"
        collective_folder = chassis_folder / collective_type
        collective_output_folder = chassis_output_folder / collective_type
        collective_folder.mkdir(parents=True, exist_ok=True)
        collective_output_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_str = "Fast" if et == 1 else "Slow"
        epoch_type_folder = collective_folder / epoch_type_str
        epoch_type_output_folder = collective_output_folder / epoch_type_str
        epoch_type_folder.mkdir(parents=True, exist_ok=True)
        epoch_type_output_folder.mkdir(parents=True, exist_ok=True)

        if et == 1 and col == 1:
            es_folder = collective_folder / "Fast_Early_Stop"
            es_output_folder = collective_output_folder / "Fast_Early_Stop"
            es_folder.mkdir(parents=True, exist_ok=True)
            es_output_folder.mkdir(parents=True, exist_ok=True)

        for tts, name in OUTPUT_TOTAL_TRANSMISSION_SIZES:
            sample_json_path = SAMPLE_INPUTS_PATH / "a800_4gpu_sample.json"
            output_json_path = epoch_type_folder / f"{name}.json"
            schedule_output_json_path = epoch_type_output_folder / f"{name}.json"
            generate_topology_helper(
                sample_json_path, output_json_path, schedule_output_json_path, 4, c, col, et, tts)

            if et == 1 and col == 1:
                output_json_path = es_folder / Path(f"{name}.json")
                schedule_output_json_path = es_output_folder / Path(f"{name}.json")
                generate_topology_helper(
                    sample_json_path, output_json_path, schedule_output_json_path, 4, c, col, et, tts, early_stop=True)


if __name__ == "__main__":
    generate_dgx2()
    generate_ndv2()
    generate_a800_4gpu()
