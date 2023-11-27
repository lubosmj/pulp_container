import subprocess
import pytest

from uuid import uuid4

from pulp_container.tests.functional.constants import (
    REGISTRY_V2,
    REGISTRY_V2_FEED_URL,
    PULP_HELLO_WORLD_REPO,
    PULP_FIXTURE_1,
)


@pytest.fixture
def pull_through_distribution(
    gen_object_with_cleanup,
    container_pull_through_remote_api,
    container_pull_through_distribution_api,
):
    remote = gen_object_with_cleanup(
        container_pull_through_remote_api,
        {"name": str(uuid4()), "url": REGISTRY_V2_FEED_URL},
    )
    distribution = gen_object_with_cleanup(
        container_pull_through_distribution_api,
        {"name": str(uuid4()), "base_path": str(uuid4()), "remote": remote.pulp_href},
    )
    return distribution


@pytest.fixture
def pull_and_verify(
    add_to_cleanup,
    container_pull_through_distribution_api,
    container_distribution_api,
    container_repository_api,
    container_remote_api,
    container_tag_api,
    registry_client,
    local_registry,
):
    def _pull_and_verify(image_path, pull_through_distribution):
        remote_image_path = f"{REGISTRY_V2}/{image_path}"
        local_image_path = f"{pull_through_distribution.base_path}/{image_path}"

        # 1. pull remote content through the Pulp distribution
        local_registry.pull(local_image_path)
        local_image = local_registry.inspect(local_image_path)

        # when the client pulls the image, a repository, distribution, and remote is created in
        # the background; therefore, scheduling the cleanup for these entities is necessary in case
        # further assertion fails
        repository = container_repository_api.list(name=local_image_path).results[0]
        add_to_cleanup(container_repository_api, repository.pulp_href)
        remote = container_remote_api.list(name=local_image_path).results[0]
        add_to_cleanup(container_remote_api, remote.pulp_href)
        distribution = container_distribution_api.list(name=local_image_path).results[0]
        add_to_cleanup(container_distribution_api, distribution.pulp_href)

        # 2. verify if the pulled content is the same as on the remote
        registry_client.pull(remote_image_path)
        remote_image = registry_client.inspect(remote_image_path)
        assert local_image[0]["Id"] == remote_image[0]["Id"]

        pull_through_distribution = container_pull_through_distribution_api.list(
            name=pull_through_distribution.name
        ).results[0]
        assert [distribution.pulp_href] == pull_through_distribution.distributions

        assert f"{repository.pulp_href}versions/1/" == repository.latest_version_href

        # 3. test if pulling the same content twice works
        local_registry.pull(local_image_path)

        repository = container_repository_api.list(name=local_image_path).results[0]
        assert f"{repository.pulp_href}versions/1/" == repository.latest_version_href

        return local_image_path

    return _pull_and_verify


def test_manifest_list_pull(
    local_registry,
    container_repository_api,
    container_tag_api,
    pull_through_distribution,
    pull_and_verify,
):
    image_path = f"{PULP_HELLO_WORLD_REPO}:latest"
    local_image_path = pull_and_verify(image_path, pull_through_distribution)

    repository = container_repository_api.list(name=local_image_path).results[0]
    tags = container_tag_api.list(repository_version=repository.latest_version_href).results
    assert ["latest"] == [tag.name for tag in tags]

    # test if pulling new content results into a new version, preserving the old content
    local_registry.pull(f"{local_image_path}:linux")

    repository = container_repository_api.list(name=local_image_path).results[0]
    assert f"{repository.pulp_href}versions/2/" == repository.latest_version_href

    tags = container_tag_api.list(repository_version=repository.latest_version_href).results
    assert ["latest", "linux"] == sorted([tag.name for tag in tags])


def test_manifest_pull(
    container_repository_api,
    container_tag_api,
    pull_through_distribution,
    pull_and_verify,
):
    image_path = f"{PULP_FIXTURE_1}:manifest_a"
    local_image_path = pull_and_verify(image_path, pull_through_distribution)

    repository = container_repository_api.list(name=local_image_path).results[0]
    tags = container_tag_api.list(repository_version=repository.latest_version_href).results
    assert ["latest"] == [tag.name for tag in tags]


def test_conflicting_names_and_paths(
    container_remote_api,
    container_remote_factory,
    container_repository_api,
    container_repository_factory,
    container_distribution_api,
    pull_through_distribution,
    gen_object_with_cleanup,
    local_registry,
    monitor_task,
):
    local_image_path = f"{pull_through_distribution.base_path}/{str(uuid4())}"

    remote = container_remote_factory(name=local_image_path)
    with pytest.raises(subprocess.CalledProcessError):
        local_registry.pull(local_image_path)
    monitor_task(container_remote_api.delete(remote.pulp_href).task)

    assert 0 == len(container_repository_api.list(name=local_image_path).results)
    assert 0 == len(container_distribution_api.list(name=local_image_path).results)

    repository = container_repository_factory(name=local_image_path)
    with pytest.raises(subprocess.CalledProcessError):
        local_registry.pull(local_image_path)
    monitor_task(container_repository_api.delete(repository.pulp_href).task)

    assert 0 == len(container_remote_api.list(name=local_image_path).results)
    assert 0 == len(container_distribution_api.list(name=local_image_path).results)

    data = {"name": local_image_path, "base_path": local_image_path}
    distribution = gen_object_with_cleanup(container_distribution_api, data)
    with pytest.raises(subprocess.CalledProcessError):
        local_registry.pull(local_image_path)
    monitor_task(container_distribution_api.delete(distribution.pulp_href).task)

    assert 0 == len(container_repository_api.list(name=local_image_path).results)
    assert 0 == len(container_remote_api.list(name=local_image_path).results)
