import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import zipfile
from collections import Counter

#sys.path.append(str(Path(__name__).resolve().parent.parent / "scripts"))
#import summarise_study_filetypes
from scripts import summarise_study_filetypes
from bia_integrator_tools.biostudies import File

FTYPES = [".tif", ".tif", ".jpg", ".jpg", ".tif", ".txt",]
N_FTYPES = len(FTYPES)

expected_zip_details = {
    k: {"n": v, "total_size": 0} 
    for k, v in dict(Counter(FTYPES)).items()
}

filelist = [
    ("f1.png", 1,), ("f2.png", 2,), ("f3.png", 3,), ("f4.png", 4,),
    ("f1.jpeg", 2), ("f2.csv", 1,), ("f3.jpeg", 4), ("f4.jpeg", 5,),
    ("f1.txt", 5,), ("f2.json", 3), ("f3.czi", 3,), ("f4.czi", 6,),
]
flist_as_files = [File(path=p, size=s) for p, s in filelist]
expected_summary = {
    ".png": {"n": 4, "total_size": 10},
    ".jpeg": {"n": 3, "total_size": 11},
    ".czi": {"n": 2, "total_size": 9},
    ".json": {"n": 1, "total_size": 3},
    ".txt": {"n": 1, "total_size": 5},
    ".csv": {"n": 1, "total_size": 1},
}
expected_zip_details = []
expected_zip_details_with_zip_added_to_suffix = []

def test_summarise_study_filetypes():
    
    summary = summarise_study_filetypes.summarise_filetypes(flist_as_files)
    assert summary == expected_summary

def test_extract_from_zip(tmp_path):
    """Test zip content info extracted ok"""
    zip_file = create_zipfile(tmp_path)
    zip_details = summarise_study_filetypes.flist_files_from_zip(zip_file)
    for z in zip_details:
        assert z in expected_zip_details

def test_extract_from_zip_with_zip_added_to_suffix(tmp_path):
    """Test zip content info extracted ok and 'zip' added to suffixes"""
    zip_file = create_zipfile(tmp_path)
    zip_details = summarise_study_filetypes.flist_files_from_zip(zip_file, add_zip_to_suffix=True)
    for z in zip_details:
        assert z in expected_zip_details_with_zip_added_to_suffix

def test_expand_path():
    """Test the expansion of a path from submission to that on filesystem"""
    to_test = [
        #(accno, submission_path, expected_expanded_path)
        ("S-BIAD9", "wormbehaviorDB/WBGene00020689/tm1498/on_food/L_2010_12_16__17_34_53___2___10/WBGene00020689_tm1498_on_food_L_2010_12_16__17_34_53___2___10_seg.avi", "/home/kola/temp/goofys/biostudies-pub/S-BIAD/S-BIAD0-99/S-BIAD9/Files/wormbehaviorDB/WBGene00020689/tm1498/on_food/L_2010_12_16__17_34_53___2___10/WBGene00020689_tm1498_on_food_L_2010_12_16__17_34_53___2___10_seg.avi"),
    ]
    for accno, submission_path, expected_expanded_path in to_test:
        expanded_path = summarise_study_filetypes._expand_path(submission_path, accno)
        assert str(expanded_path) == expected_expanded_path

def create_zipfile(tmp_path):
    """Create a temporary zipfile to work with"""
    
    fpaths = []
    for fname, fsize in filelist:
        # Create a file with an arbitrary number of bytes
        fpath = tmp_path / fname
        # Strip leading slash from Path as absent in zipfile
        zip_fpath = Path(str(tmp_path)[1:]) / fname
        zip_fpath2 = Path(str(tmp_path)[1:]) / fname.replace(".", ".zip_")
        # Save to temp file
        fsize = fpath.write_text("x" * fsize)
        expected_zip_details.append(File(path=zip_fpath, size=fsize))
        expected_zip_details_with_zip_added_to_suffix.append(File(path=zip_fpath2, size=fsize))
        fpaths.append(str(fpath))
    
    zip_file = str(tmp_path / "test.zip")
    with zipfile.ZipFile(zip_file, mode="a") as archive:
        for fpath in fpaths:
            # Add to zip archive
            archive.write(fpath)

    return zip_file

