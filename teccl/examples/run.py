import glob
import subprocess

# for topology in NDv2 DGX2 A800_4GPU;
for topology in ["NDv2", "DGX2", "A800_4GPU"]:

    # for chassis in experiments/"$topology"*/;
    for chassis in glob.glob(f"experiments/{topology}*/"):

        # for collective in "$chassis"*/;
        for collective in glob.glob(f"{chassis}*/"):

            # for epoch_type in "$collective"*/;
            for epoch_type in glob.glob(f"{collective}*/"):

                # for config_file in $epoch_type*/*;
                for config_file in glob.glob(f"{epoch_type}*/*"):

                    print(f"Running teccl solve --input_args {config_file}")

                    subprocess.run([
                        "teccl",
                        "solve",
                        "--input_args",
                        config_file
                    ])
