"""Convert a json file with BIA submission metadata to tsv

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
accno\ttitle\tn_filetypes\ttotal_no_of_files\ttotal_file_size\tfiletype_breakdown\n

"""

import sys
from pathlib import Path
import json

input_path = sys.argv[1]
with open(input_path, "r") as fid:
    submission_details = json.load(fid)

out_details = ["accno\ttitle\tn_filetypes\ttotal_no_of_files\ttotal_file_size\tfiletype_breakdown",]

for accno, details in submission_details.items():
    if len(details["filetypes"].keys()) == 0:
        title = ""
        n_filetypes = 0
        total_no_of_files = 0
        total_file_size = 0
        filetype_breakdown = ""
    else:
        title = details['title'].replace("\n"," ").replace("\t"," ")
        n_filetypes = len(details["filetypes"].keys())
        total_no_of_files = sum([v["n"] for v in details["filetypes"].values()])
        total_file_size = sum([v["total_size"] for v in details["filetypes"].values()])
        filetype_breakdown = f"{details['filetypes']}"
    line = (
        f"{accno}\t{title}\t{n_filetypes}\t{total_no_of_files}\t"
        f"{total_file_size}\t{filetype_breakdown}"
    )
    out_details.append(line)

output_path = Path(input_path).stem + ".tsv"
Path(output_path).write_text("\n".join(out_details))
print(f"Converted {input_path} to {output_path}")
