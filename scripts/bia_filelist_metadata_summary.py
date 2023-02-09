"""Aggregate metadata info over all accnos

json format ->
{
    "accno1": {
        "title": "title of submission",
        "filetypes": {
            "type1": {
                "n": 10,
                "total_size": 1314234
            },
            "type2": {
                "n": 123,
                "total_size": 13222
            }
        }
    },
    "accno2": {
        ...
    },
    ...
    "accnoN": {
        ...
    }
}

tsv format ->
filetype\tn_accnos\ttotal_no_of_files\ttotal_file_size\n

"""

import sys
from pathlib import Path
import json

input_path = sys.argv[1]
with open(input_path, "r") as fid:
    submission_details = json.load(fid)

out_details = ["filetype\tn_accnos\ttotal_no_of_files\ttotal_file_size\taccnos"]
summary_details = {}
for accno, details in submission_details.items():
    if len(details["filetypes"].keys()) == 0:
        ext_name = "No files"
        if ext_name in summary_details:
            summary_details[ext_name]["accnos"].append(accno)
        else:
            summary_details[ext_name] = {}
            summary_details[ext_name]["accnos"] = [accno,]
            summary_details[ext_name]["total_no_of_files"] = 0
            summary_details[ext_name]["total_file_size"] = 0
    else:
        for ext_name, ext_details in details["filetypes"].items():
            if ext_name in summary_details:
                summary_details[ext_name]["accnos"].append(accno)
                summary_details[ext_name]["total_no_of_files"] += ext_details["n"]
                summary_details[ext_name]["total_file_size"] += ext_details["total_size"]
            else:
                summary_details[ext_name] = {}
                summary_details[ext_name]["accnos"] = [accno,]
                summary_details[ext_name]["total_no_of_files"] = ext_details["n"]
                summary_details[ext_name]["total_file_size"] = ext_details["total_size"]

for key, values in summary_details.items():
    line = [key, str(len(values["accnos"])),]
    list_values = list(values.values())
    line.extend([str(v) for v in list_values[1:]])
    line.append(",".join(list_values[0]))
    out_details.append("\t".join(line))

output_path = Path(input_path).stem + ".tsv"
Path(output_path).write_text("\n".join(out_details))
print(f"Converted {input_path} to {output_path}")
