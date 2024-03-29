import logging

logger = logging.getLogger("biaint")
logging.basicConfig(level=logging.INFO)

import typer

from bia_integrator_core.models import (
    StudyAnnotation,
    BIAImageRepresentation,
    ImageAnnotation,
    BIACollection,
    StudyTag,
    BIAImageAlias
)

from bia_integrator_core.interface import (
    get_study,
    get_all_study_identifiers,
    get_image,
    get_images_for_study,
    get_study_annotations,
    persist_study_annotation,
    persist_image_representation,
    persist_image_annotation,
    persist_collection,
    get_study_tags,
    persist_study_tag,
    get_collection,
    get_aliases,
    persist_image_alias
)
from bia_integrator_core.integrator import load_and_annotate_study


app = typer.Typer()

aliases_app = typer.Typer()
app.add_typer(aliases_app, name="aliases")

images_app = typer.Typer()
app.add_typer(images_app, name="images")

studies_app = typer.Typer()
app.add_typer(studies_app, name="studies")

reps_app = typer.Typer()
app.add_typer(reps_app, name="representations")

collections_app = typer.Typer()
app.add_typer(collections_app, name="collections")

annotations_app = typer.Typer()
app.add_typer(annotations_app, name="annotations")

filerefs_app = typer.Typer()
app.add_typer(filerefs_app, name="filerefs")


@aliases_app.command("add")
def add_alias(accession_id: str, image_id: str, name: str):
    alias = BIAImageAlias(
        accession_id=accession_id,
        image_id=image_id,
        name=name
    )

    persist_image_alias(alias)


@aliases_app.command("list")
def list_aliases(accession_id: str, image_id: str):
    aliases = get_aliases(accession_id)

    for alias in aliases:
        if image_id == alias.image_id:
            print(alias.name, alias.accession_id, alias.image_id)


@aliases_app.command("list-for-study")
def list_aliases_for_study(accession_id: str):
    aliases = get_aliases(accession_id)

    for alias in aliases:
        print(alias.name, alias.accession_id, alias.image_id)


@filerefs_app.command("list")
def list_filerefs(accession_id: str):
    bia_study = load_and_annotate_study(accession_id)

    for fileref in bia_study.file_references.values():
        print(fileref.id, fileref.name, fileref.size_in_bytes)


@images_app.command("list")
def images_list(accession_id: str):
    images = get_images_for_study(accession_id)

    for image in images:
        rep_rep = ','.join(rep.type for rep in image.representations)
        typer.echo(f"{image.id} {image.original_relpath} {rep_rep}")


@images_app.command("show")
def images_show(accession_id: str, image_id: str):
    study = load_and_annotate_study(accession_id)
    image = study.images[image_id]

    typer.echo(image.id)
    typer.echo(image.original_relpath)
    typer.echo(f"Dimensions: {image.dimensions}")
    typer.echo("Attributes:")
    for k, v in image.attributes.items():
        typer.echo(f"  {k}={v}")
    typer.echo("Representations:")
    for rep in image.representations:
        typer.echo(f"  {rep}")

    
@studies_app.command("show")
def show(accession_id: str):
    study = load_and_annotate_study(accession_id)

    typer.echo(study)


@studies_app.command("list")
def list():
    studies = get_all_study_identifiers()

    typer.echo('\n'.join(sorted(studies)))


@annotations_app.command("list-studies")
def list_study_annotations(accession_id: str):
    annotations = get_study_annotations(accession_id)

    typer.echo(annotations)


@annotations_app.command("create-study")
def create_study_annotation(accession_id: str, key: str, value: str):

    annotation = StudyAnnotation(
        accession_id=accession_id,
        key=key,
        value=value
    )

    persist_study_annotation(annotation)


@annotations_app.command("create-image")
def create_image_annotation(accession_id: str, image_id: str, key: str, value: str):
    annotation = ImageAnnotation(
        accession_id=accession_id,
        image_id=image_id,
        key=key,
        value=value
    )

    persist_image_annotation(annotation)


@annotations_app.command("list-study-tags")
def list_study_tags(accession_id):
    tags = get_study_tags(accession_id)

    typer.echo(tags)


@annotations_app.command("create-study-tag")
def create_study_tag(accession_id, value):
    tag = StudyTag(
        accession_id=accession_id,
        value=value
    )
    persist_study_tag(tag)


@reps_app.command("register")
def register_image_representation(accession_id: str, image_id: str, type: str, size: int, uri: str):
    rep = BIAImageRepresentation(
        accession_id=accession_id,
        image_id=image_id,
        type=type,
        uri=uri,
        size=size,
        dimensions=None,
        attributes={}
    )
    persist_image_representation(rep)


@collections_app.command("create")
def create_collection(name: str, title: str, subtitle: str, accessions_list: str):
    collection = BIACollection(
        name=name,  
        title=title,
        subtitle=subtitle,
        accession_ids=accessions_list.split(","),
        description=None
    )
    persist_collection(collection)


@collections_app.command("add-study")
def add_study_to_collection(collection_name: str, accession_id: str):
    collection = get_collection(collection_name)
    
    collection.accession_ids.append(accession_id)

    persist_collection(collection)

    


if __name__ == "__main__":
    app()