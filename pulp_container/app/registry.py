import time
import json
import logging
import os

from asgiref.sync import sync_to_async
from tempfile import NamedTemporaryFile

from contextlib import suppress
from urllib.parse import urljoin

from aiohttp import web
from django_guid import set_guid
from django_guid.utils import generate_guid
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from multidict import MultiDict

from pulpcore.plugin.content import Handler, PathNotResolved
from pulpcore.plugin.models import Artifact, RemoteArtifact, Content, ContentArtifact, Task
from pulpcore.plugin.content import ArtifactResponse
from pulpcore.plugin.tasking import dispatch

from pulp_container.app.cache import RegistryContentCache
from pulp_container.app.models import ContainerDistribution, Tag, Blob, Manifest, BlobManifest
from pulp_container.app.schema_convert import Schema2toSchema1ConverterWrapper
from pulp_container.app.tasks import download_image_data
from pulp_container.app.utils import (
    calculate_digest,
    get_accepted_media_types,
    determine_media_type,
)
from pulp_container.constants import BLOB_CONTENT_TYPE, EMPTY_BLOB, MEDIA_TYPE, V2_ACCEPT_HEADERS

log = logging.getLogger(__name__)


class Registry(Handler):
    """
    A set of handlers for the Container Registry v2 API.
    """

    distribution_model = ContainerDistribution

    @staticmethod
    def _base_paths(path):
        """
        Get a list of base paths used to match a distribution.

        Args:
            path (str): The path component of the URL.

        Returns:
            list: Of base paths.

        """
        return [path]

    @staticmethod
    async def _dispatch(artifact, headers):
        """
        Stream a file back to the client.

        Stream the bits.

        Args:
            artifact (:class:`pulpcore.app.models.Artifact`): Artifact to respond with
            headers (dict): A dictionary of response headers.

        Raises:
            :class:`aiohttp.web_exceptions.HTTPFound`: When we need to redirect to the file
            NotImplementedError: If file is stored in a file storage we can't handle

        Returns:
            The :class:`aiohttp.web.StreamedResponse` for the Artifact.

        """
        full_headers = MultiDict()

        full_headers["Content-Type"] = headers["Content-Type"]
        full_headers["Docker-Content-Digest"] = headers["Docker-Content-Digest"]
        full_headers["Docker-Distribution-API-Version"] = "registry/2.0"

        if settings.DEFAULT_FILE_STORAGE == "pulpcore.app.models.storage.FileSystem":
            file = artifact.file
            path = os.path.join(settings.MEDIA_ROOT, file.name)
            if not os.path.exists(path):
                raise Exception("Expected path '{}' is not found".format(path))
            return web.FileResponse(path, headers=full_headers)
        elif not settings.REDIRECT_TO_OBJECT_STORAGE:
            return ArtifactResponse(artifact=artifact, headers=headers)
        else:
            raise NotImplementedError("Redirecting to this storage is not implemented.")

    @RegistryContentCache(
        base_key=lambda req, cac: Registry.find_base_path_cached(req, cac),
        auth=lambda req, cac, bk: Registry.auth_cached(req, cac, bk),
    )
    async def get_tag(self, request):
        """
        Match the path and stream either Manifest or ManifestList.

        Args:
            request(:class:`~aiohttp.web.Request`): The request to prepare a response for.

        Raises:
            PathNotResolved: The path could not be matched to a published file.
            PermissionError: When not permitted.

        Returns:
            :class:`aiohttp.web.StreamResponse` or :class:`aiohttp.web.FileResponse`: The response
                streamed back to the client.

        """

        path = request.match_info["path"]
        tag_name = request.match_info["tag_name"]
        distribution = await sync_to_async(self._match_distribution)(path, add_trailing_slash=False)
        await sync_to_async(self._permit)(request, distribution)
        repository_version = await sync_to_async(distribution.get_repository_version)()
        if not repository_version:
            raise PathNotResolved(tag_name)
        accepted_media_types = get_accepted_media_types(request.headers)

        try:
            tag = await Tag.objects.select_related("tagged_manifest").aget(
                pk__in=await sync_to_async(repository_version.get_content)(), name=tag_name
            )
        except ObjectDoesNotExist:
            if distribution.remote:
                remote = await distribution.remote.acast()

                relative_url = "/v2/{name}/manifests/{tag}".format(
                    name=remote.namespaced_upstream_name, tag=tag_name
                )
                tag_url = urljoin(remote.url, relative_url)
                downloader = remote.get_in_memory_downloader(url=tag_url)
                response = await downloader.run(extra_data={"headers": V2_ACCEPT_HEADERS})

                set_guid(generate_guid())
                await sync_to_async(dispatch)(
                    download_image_data,
                    exclusive_resources=[repository_version.repository],
                    kwargs={
                        "repository_pk": repository_version.repository.pk,
                        "remote_pk": remote.pk,
                        "tag_name": tag_name,
                        "response_data": response.data,
                    },
                    immediate=True,
                    deferred=True,
                )

                try:
                    manifest_data = json.loads(response.data)
                except json.decoder.JSONDecodeError:
                    raise PathNotResolved(tag_name)
                else:
                    media_type = determine_media_type(manifest_data, response)
                    if media_type in (MEDIA_TYPE.MANIFEST_V1_SIGNED, MEDIA_TYPE.MANIFEST_V1):
                        encoded_data = response.data.encode("utf-8")
                        digest = calculate_digest(encoded_data)
                    else:
                        # digest = response.artifact_attributes["sha256"]
                        encoded_data = response.data.encode("utf-8")
                        digest = calculate_digest(encoded_data)

                    # TODO: consider saving a manifest list too
                    if media_type not in (MEDIA_TYPE.MANIFEST_LIST, MEDIA_TYPE.INDEX_OCI):
                        config_digest = manifest_data["config"]["digest"]
                        config_blob = await self.save_config_blob(config_digest, remote)
                        manifest = Manifest(
                            digest=digest,
                            schema_version=2,
                            media_type=media_type,
                            config_blob=config_blob,
                        )
                        try:
                            await manifest.asave()
                        except IntegrityError:
                            manifest = await Manifest.objects.aget(digest=manifest.digest)
                            await sync_to_async(manifest.touch)()

                        ras_to_create = []
                        cas_to_create = []
                        bm_rels_to_create = []
                        for layer in manifest_data["layers"]:
                            digest = layer["digest"]

                            blob = Blob(digest=digest)
                            try:
                                await blob.asave()
                            except IntegrityError:
                                blob = await Blob.objects.aget(digest=digest)
                                await sync_to_async(blob.touch)()

                            bm_rel = BlobManifest(manifest=manifest, manifest_blob=blob)
                            with suppress(IntegrityError):
                                await bm_rel.asave()
                            bm_rels_to_create.append(bm_rel)

                            ca = ContentArtifact(
                                content=blob,
                                artifact=None,
                                relative_path=digest,
                            )
                            with suppress(IntegrityError):
                                await ca.asave()
                            cas_to_create.append(ca)

                            relative_url = "/v2/{name}/blobs/{digest}".format(
                                name=remote.namespaced_upstream_name, digest=digest
                            )
                            blob_url = urljoin(remote.url, relative_url)
                            ra = RemoteArtifact(
                                url=blob_url,
                                sha256=digest[len("sha256:") :],
                                content_artifact=ca,
                                remote=remote,
                            )
                            with suppress(IntegrityError):
                                await ra.asave()
                            ras_to_create.append(ra)

                        if ras_to_create:
                            # in pulpcore, we usually care about the order to prevent deadlocks;
                            # having only a small set of blobs here might not cause any troubles
                            pass
                            #await sync_to_async(
                            #    BlobManifest.objects.bulk_create
                            #)(bm_rels_to_create, ignore_conflicts=True)
                            #await sync_to_async(
                            #    ContentArtifact.objects.bulk_create
                            #)(cas_to_create, ignore_conflicts=True)
                            #await sync_to_async(
                            #    RemoteArtifact.objects.bulk_create
                            #)(ras_to_create, ignore_conflicts=True)

                        with NamedTemporaryFile(
                            mode="w", dir=settings.WORKING_DIRECTORY, delete=False
                        ) as tmp_file:
                            tmp_file.write(response.data)
                            tmp_file.flush()
                            try:
                                artifact = Artifact.init_and_validate(tmp_file.name)
                                await artifact.asave()
                            except IntegrityError:
                                artifact = await Artifact.objects.aget(sha256=artifact.sha256)
                                await sync_to_async(artifact.touch)()

                        content_artifact = ContentArtifact(
                            artifact=artifact, content=manifest, relative_path=manifest.digest
                        )
                        with suppress(IntegrityError):
                            await content_artifact.asave()

                response_headers = {
                    "Content-Type": media_type,
                    "Docker-Content-Digest": digest,
                    "Docker-Distribution-API-Version": "registry/2.0",
                }

                # at this time, the manifest artifact was already established, and we can return it
                # as it is; meanwhile, the dispatched task has created Manifest/Blob objects and
                # relations between them; the said content units are streamed/downloaded on demand
                # to a client on a next run
                return web.Response(text=response.data, headers=response_headers)
            else:
                raise PathNotResolved(tag_name)
        else:
            if distribution.remote and distribution.pull_through_distribution_id:
                # check if the content was updated on the remove and stream it back
                remote = await distribution.remote.acast()
                relative_url = "/v2/{name}/manifests/{tag}".format(
                    name=remote.namespaced_upstream_name, tag=tag_name
                )
                tag_url = urljoin(remote.url, relative_url)
                downloader = remote.get_in_memory_downloader(url=tag_url)
                response = await downloader.run(extra_data={"headers": V2_ACCEPT_HEADERS})

                try:
                    manifest_data = json.loads(response.data)
                except json.decoder.JSONDecodeError:
                    raise PathNotResolved(tag_name)

                media_type = determine_media_type(manifest_data, response)
                if media_type in (MEDIA_TYPE.MANIFEST_V1_SIGNED, MEDIA_TYPE.MANIFEST_V1):
                    encoded_data = response.data.encode("utf-8")
                    digest = calculate_digest(encoded_data)
                else:
                    # TODO: in_memory_downloader does not have artifact_attributes
                    encoded_data = response.data.encode("utf-8")
                    digest = calculate_digest(encoded_data)
                    # digest = response.artifact_attributes["sha256"]

                if tag.manifest.digest != digest:
                    set_guid(generate_guid())
                    await sync_to_async(dispatch)(
                        download_image_data,
                        exclusive_resources=[repository_version.repository],
                        kwargs={
                            "repository_pk": repository_version.repository.pk,
                            "remote_pk": remote.pk,
                            "tag_name": tag_name,
                            "response_data": response.data,
                        },
                        immediate=True,
                        deferred=True,
                    )

        # we do not convert OCI to docker
        oci_mediatypes = [MEDIA_TYPE.MANIFEST_OCI, MEDIA_TYPE.INDEX_OCI]
        if (
            tag.tagged_manifest.media_type in oci_mediatypes
            and tag.tagged_manifest.media_type not in accepted_media_types
        ):
            log.warn(
                "OCI format found, but the client only accepts {accepted_media_types}.".format(
                    accepted_media_types=accepted_media_types
                )
            )
            raise PathNotResolved(tag_name)

        # return schema1 (even in case only oci is requested)
        if tag.tagged_manifest.media_type == MEDIA_TYPE.MANIFEST_V1:
            return_media_type = MEDIA_TYPE.MANIFEST_V1_SIGNED
            response_headers = {
                "Content-Type": return_media_type,
                "Docker-Content-Digest": tag.tagged_manifest.digest,
            }
            return await self.dispatch_tag(request, tag, response_headers)

        # return what was found in case media_type is accepted header (docker, oci)
        if tag.tagged_manifest.media_type in accepted_media_types:
            return_media_type = tag.tagged_manifest.media_type
            response_headers = {
                "Content-Type": return_media_type,
                "Docker-Content-Digest": tag.tagged_manifest.digest,
            }
            return await self.dispatch_tag(request, tag, response_headers)

        # convert if necessary
        return await Registry.dispatch_converted_schema(tag, accepted_media_types, path)

    async def save_config_blob(self, config_digest, remote):
        relative_url = "/v2/{name}/blobs/{digest}".format(
            name=remote.namespaced_upstream_name, digest=config_digest
        )
        blob_url = urljoin(remote.url, relative_url)
        downloader = remote.get_in_memory_downloader(url=blob_url)
        response = await downloader.run()

        with NamedTemporaryFile(mode="w", dir=settings.WORKING_DIRECTORY, delete=False) as tmp_file:
            tmp_file.write(response.data)
            tmp_file.flush()
            try:
                config_blob_artifact = Artifact.init_and_validate(tmp_file.name)
                await config_blob_artifact.asave()
                assert (
                    config_blob_artifact.sha256 == config_digest[len("sha256:") :]
                )
            except IntegrityError:
                config_blob_artifact = await Artifact.objects.aget(
                    sha256=config_blob_artifact.sha256
                )
                await sync_to_async(config_blob_artifact.touch)()

        config_blob = Blob(digest=config_digest)
        try:
            await config_blob.asave()
        except IntegrityError:
            pass

        content_artifact = ContentArtifact(
            content=config_blob,
            artifact=config_blob_artifact,
            relative_path=config_digest,
        )
        with suppress(IntegrityError):
            await content_artifact.asave()

        return config_blob

    async def dispatch_tag(self, request, tag, response_headers):
        """
        Finds an artifact associated with a Tag and sends it to the client, otherwise tries
        to stream it.

        Args:
            request(:class:`~aiohttp.web.Request`): The request to prepare a response for.
            tag: Tag
            response_headers (dict): dictionary that contains the 'Content-Type' header to send
                with the response

        Returns:
            :class:`aiohttp.web.StreamResponse` or :class:`aiohttp.web.FileResponse`: The response
                streamed back to the client.

        """
        try:
            artifact = await tag.tagged_manifest._artifacts.aget()
        except ObjectDoesNotExist:
            ca = await sync_to_async(lambda x: x[0])(tag.tagged_manifest.contentartifact_set.all())
            return await self._stream_content_artifact(request, web.StreamResponse(), ca)
        else:
            return await Registry._dispatch(artifact, response_headers)

    @staticmethod
    async def dispatch_converted_schema(tag, accepted_media_types, path):
        """
        Convert a manifest from the format schema 2 to the format schema 1.

        The format is converted on-the-go and created resources are not stored for further uses.
        The conversion is made after each request which does not accept the format for schema 2.

        Args:
            tag: A tag object which contains reference to tagged manifests and config blobs.
            accepted_media_types: Accepted media types declared in the accept header.
            path: A path of a repository.

        Raises:
            PathNotResolved: There was not found a valid conversion for the specified tag.

        Returns:
            :class:`aiohttp.web.StreamResponse` or :class:`aiohttp.web.Response`: The response
                streamed back to the client.

        """
        schema1_converter = Schema2toSchema1ConverterWrapper(tag, accepted_media_types, path)
        try:
            result = await sync_to_async(schema1_converter.convert)()
        except RuntimeError:
            raise PathNotResolved(tag.name)

        response_headers = {
            "Docker-Content-Digest": result.digest,
            "Content-Type": result.content_type,
            "Docker-Distribution-API-Version": "registry/2.0",
        }
        return web.Response(text=result.text, headers=response_headers)

    @RegistryContentCache(
        base_key=lambda req, cac: Registry.find_base_path_cached(req, cac),
        auth=lambda req, cac, bk: Registry.auth_cached(req, cac, bk),
    )
    async def get_by_digest(self, request):
        """
        Return a response to the "GET" action.
        """
        path = request.match_info["path"]
        digest = "sha256:{digest}".format(digest=request.match_info["digest"])
        distribution = await sync_to_async(self._match_distribution)(path, add_trailing_slash=False)
        await sync_to_async(self._permit)(request, distribution)
        repository_version = await sync_to_async(distribution.get_repository_version)()
        if not repository_version:
            raise PathNotResolved(path)
        if digest == EMPTY_BLOB:
            return await Registry._empty_blob()
        try:
            content = await sync_to_async(repository_version.get_content)()

            repository = await sync_to_async(repository_version.repository.cast)()
            pending_blobs = repository.pending_blobs.values_list("pk")
            pending_manifests = repository.pending_manifests.values_list("pk")
            pending_content = pending_blobs.union(pending_manifests)
            content |= Content.objects.filter(pk__in=pending_content)

            ca = await ContentArtifact.objects.select_related("artifact", "content").aget(
                content__in=content, relative_path=digest
            )
            ca_content = await sync_to_async(ca.content.cast)()
            if isinstance(ca_content, Blob):
                media_type = BLOB_CONTENT_TYPE
            else:
                media_type = ca_content.media_type
            headers = {
                "Content-Type": media_type,
                "Docker-Content-Digest": ca_content.digest,
            }
        except ObjectDoesNotExist:
            raise PathNotResolved(path)
        else:
            artifact = ca.artifact
            if artifact:
                return await Registry._dispatch(artifact, headers)
            else:
                return await self._stream_content_artifact(request, web.StreamResponse(), ca)

    @staticmethod
    async def _empty_blob():
        # fmt: off
        empty_tar = [
            31, 139, 8, 0, 0, 9, 110, 136, 0, 255, 98, 24, 5, 163, 96, 20, 140, 88, 0, 8, 0, 0, 255,
            255, 46, 175, 181, 239, 0, 4, 0, 0,
        ]
        # fmt: on
        body = bytes(empty_tar)
        response_headers = {
            "Docker-Content-Digest": EMPTY_BLOB,
            "Content-Type": BLOB_CONTENT_TYPE,
            "Docker-Distribution-API-Version": "registry/2.0",
        }
        return web.Response(body=body, headers=response_headers)
