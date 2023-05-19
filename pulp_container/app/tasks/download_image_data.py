import asyncio
import json
import logging

from tempfile import NamedTemporaryFile

from asgiref.sync import sync_to_async

from django.db import IntegrityError

from pulpcore.plugin.models import Artifact
from pulpcore.plugin.stages import (
    ArtifactDownloader,
    ArtifactSaver,
    DeclarativeContent,
    DeclarativeVersion,
    RemoteArtifactSaver,
    ResolveContentFutures,
    QueryExistingArtifacts,
    QueryExistingContents,
)

from pulp_container.app.models import ContainerRemote, ContainerRepository, Tag
from pulp_container.app.utils import determine_media_type_from_json
from pulp_container.constants import MEDIA_TYPE

from .sync_stages import ContainerContentSaver, ContainerFirstStage

log = logging.getLogger(__name__)


def download_image_data(repository_pk, remote_pk, tag_name, response_data):
    repository = ContainerRepository.objects.get(pk=repository_pk)
    remote = ContainerRemote.objects.get(pk=remote_pk).cast()
    log.info("Pulling cache: repository={r} remote={p}".format(r=repository.name, p=remote.name))
    first_stage = ContainerPullThroughFirstStage(remote, tag_name, response_data)
    dv = ContainerPullThroughCacheDeclarativeVersion(first_stage, repository, mirror=False)
    return dv.create()


class ContainerPullThroughFirstStage(ContainerFirstStage):
    """The stage that prepares the pipeline for downloading a specific tag and its data."""

    def __init__(self, remote, tag_name, response_data):
        """Initialize the stage."""
        super().__init__(remote, signed_only=False)
        self.tag_name = tag_name
        self.response_data = response_data

    async def run(self):
        """Run the stage and set declarative content for one tag, its manifest, and blobs."""
        tag_dc = DeclarativeContent(Tag(name=self.tag_name))

        content_data = json.loads(self.response_data)
        with NamedTemporaryFile("w") as temp_file:
            temp_file.write(self.response_data)
            temp_file.flush()

            artifact = Artifact.init_and_validate(temp_file.name)
            try:
                await artifact.asave()
            except IntegrityError:
                artifact = await Artifact.objects.aget(sha256=artifact.sha256)
                await sync_to_async(artifact.touch)()

        media_type = determine_media_type_from_json(content_data)
        if media_type in (MEDIA_TYPE.MANIFEST_LIST, MEDIA_TYPE.INDEX_OCI):
            list_dc = self.create_tagged_manifest_list(
                self.tag_name, artifact, content_data, media_type
            )
            for listed_manifest_task in asyncio.as_completed(
                [
                    self.create_listed_manifest(manifest_data)
                    for manifest_data in content_data.get("manifests")
                ]
            ):
                listed_manifest = await listed_manifest_task
                man_dc = listed_manifest["manifest_dc"]
                list_dc.extra_data["listed_manifests"].append(listed_manifest)
            else:
                tag_dc.extra_data["tagged_manifest_dc"] = list_dc
                for listed_manifest in list_dc.extra_data["listed_manifests"]:
                    await self.handle_blobs(
                        listed_manifest["manifest_dc"], listed_manifest["content_data"]
                    )
                    self.manifest_dcs.append(listed_manifest["manifest_dc"])
                self.manifest_list_dcs.append(list_dc)
        else:
            # Simple tagged manifest
            man_dc = self.create_tagged_manifest(
                self.tag_name, artifact, content_data, self.response_data, media_type
            )
            tag_dc.extra_data["tagged_manifest_dc"] = man_dc
            await self.handle_blobs(man_dc, content_data)
            self.manifest_dcs.append(man_dc)

        for manifest_dc in self.manifest_dcs:
            config_blob_dc = manifest_dc.extra_data.get("config_blob_dc")
            if config_blob_dc:
                manifest_dc.content.config_blob = await config_blob_dc.resolution()
            for blob_dc in manifest_dc.extra_data["blob_dcs"]:
                # Just await here. They will be associated in the post_save hook.
                await blob_dc.resolution()
            await self.put(manifest_dc)
        self.manifest_dcs.clear()

        for manifest_list_dc in self.manifest_list_dcs:
            for listed_manifest in manifest_list_dc.extra_data["listed_manifests"]:
                # Just await here. They will be associated in the post_save hook.
                await listed_manifest["manifest_dc"].resolution()
            await self.put(manifest_list_dc)
        self.manifest_list_dcs.clear()

        tagged_manifest_dc = tag_dc.extra_data["tagged_manifest_dc"]
        tag_dc.content.tagged_manifest = await tagged_manifest_dc.resolution()
        await self.put(tag_dc)


class ContainerPullThroughCacheDeclarativeVersion(DeclarativeVersion):
    """
    Subclassed Declarative version that creates a pipeline for caching remote content.
    """

    def pipeline_stages(self, new_version):
        """
        Define the "architecture" of caching remote content.

        Args:
            new_version (:class:`~pulpcore.plugin.models.RepositoryVersion`): The
                new repository version that is going to be built.

        Returns:
            list: List of :class:`~pulpcore.plugin.stages.Stage` instances

        """
        pipeline = [
            self.first_stage,
            QueryExistingArtifacts(),
            ArtifactDownloader(),
            ArtifactSaver(),
            QueryExistingContents(),
            ContainerContentSaver(),
            RemoteArtifactSaver(),
            ResolveContentFutures(),
        ]

        return pipeline
