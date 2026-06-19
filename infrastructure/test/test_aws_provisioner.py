import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from setup.aws_provisioner import AWSProvisioner
from setup.cli_executor import ErrorCategory

_TEST_LOGGER = logging.getLogger("test.aws_provisioner")


@pytest.fixture
def aws_provisioner(mock_cli, mock_env_writer):
    return AWSProvisioner(cli=mock_cli, env=mock_env_writer, logger=_TEST_LOGGER)


class TestGetRegion:
    def test_region_from_cli(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": True, "stdout": "  us-west-2  \n"}

        assert aws_provisioner._get_region() == "us-west-2"

    def test_region_from_env_fallback(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": ""}

        with patch.dict("os.environ", {"AWS_REGION": "eu-west-1"}):
            assert aws_provisioner._get_region() == "eu-west-1"

    def test_region_default(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": ""}

        assert aws_provisioner._get_region() == "us-east-1"


class TestCreateS3Lake:
    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_success(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}

        result = aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        assert result["ok"] is True
        assert result["message"] == "created"
        assert result["bucket"] == "my-bucket"

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_already_exists_reuse(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "BucketAlreadyOwnedByYou",
            "error_category": ErrorCategory.ALREADY_EXISTS, "remedy": "reuse",
        }
        mock_cli.prompt_choice.return_value = "Reuse existing s3 bucket"

        result = aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        assert result["ok"] is True
        assert result["message"] == "already_exists"

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_already_exists_new_name(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": False, "stderr": "BucketAlreadyOwnedByYou",
                "error_category": ErrorCategory.ALREADY_EXISTS, "remedy": "reuse",
            },
            {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""},
        ]
        mock_cli.prompt_choice.return_value = "Enter a new name"
        mock_cli.prompt_input.return_value = "my-bucket-v2"

        result = aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        assert result["ok"] is True
        assert result["bucket"] == "my-bucket-v2"

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_unauthorized(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "AccessDenied",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "add IAM policy",
        }

        result = aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        assert result["ok"] is False
        assert result["message"] == "unauthorized"

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_auth_expired(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "RequestExpired",
            "error_category": ErrorCategory.AUTH_EXPIRED, "remedy": "re-auth",
        }

        result = aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        assert result["ok"] is False
        assert result["message"] == "auth_expired"

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_invalid_name(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "InvalidBucketName",
            "error_category": ErrorCategory.INVALID_NAME, "remedy": "fix name",
        }

        result = aws_provisioner.create_s3_lake("BAD_NAME", "us-east-1")

        assert result["ok"] is False

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_non_us_east_1_adds_location_constraint(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}

        aws_provisioner.create_s3_lake("my-bucket", "eu-west-1")

        cmd = mock_cli.execute.call_args[0][0]
        assert "--create-bucket-configuration" in cmd

    @patch("setup.aws_provisioner.Spinner")
    def test_create_bucket_us_east_1_no_constraint(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}

        aws_provisioner.create_s3_lake("my-bucket", "us-east-1")

        cmd = mock_cli.execute.call_args[0][0]
        assert "--create-bucket-configuration" not in cmd


class TestStructureLakeLayers:
    @patch("setup.aws_provisioner.Spinner")
    def test_all_layers_created(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""}

        result = aws_provisioner.structure_lake_layers("my-bucket", ["bronze", "silver"])

        assert result["ok"] is True
        assert result["created"] == ["bronze", "silver"]
        assert result["failed"] == []

    @patch("setup.aws_provisioner.Spinner")
    def test_some_layers_fail(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": False, "stdout": "", "stderr": "AccessDenied",
                "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
            },
        ]

        result = aws_provisioner.structure_lake_layers("my-bucket", ["bronze", "silver"])

        assert result["ok"] is False
        assert result["created"] == ["bronze"]
        assert result["failed"] == ["silver"]

    @patch("setup.aws_provisioner.Spinner")
    def test_empty_layers(self, mock_spinner_cls, aws_provisioner, mock_cli):
        result = aws_provisioner.structure_lake_layers("my-bucket", [])

        assert result["ok"] is True
        mock_cli.execute.assert_not_called()


class TestProvisionComputeVm:
    @patch("setup.aws_provisioner.Spinner")
    def test_vm_launch_success(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"Instances": [{"InstanceId": "i-12345"}]}),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = aws_provisioner.provision_compute_vm(bootstrap=True)

        assert result["ok"] is True
        assert result["instance_id"] == "i-12345"

    @patch("setup.aws_provisioner.Spinner")
    def test_vm_launch_with_local_postgres(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"Instances": [{"InstanceId": "i-999"}]}),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = aws_provisioner.provision_compute_vm(
            bootstrap=True, install_local_postgres=True, dw_password="mypass123"
        )

        assert result["ok"] is True
        cmd = mock_cli.execute.call_args[0][0]
        assert "--user-data" in cmd

    @patch("setup.aws_provisioner.Spinner")
    def test_vm_launch_unauthorized(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "UnauthorizedOperation",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = aws_provisioner.provision_compute_vm()

        assert result["ok"] is False
        assert result["message"] == "unauthorized"

    @patch("setup.aws_provisioner.Spinner")
    def test_vm_launch_auth_expired(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "TokenExpired",
            "error_category": ErrorCategory.AUTH_EXPIRED, "remedy": "expired",
        }

        result = aws_provisioner.provision_compute_vm()

        assert result["ok"] is False
        assert result["message"] == "auth_expired"

    @patch("setup.aws_provisioner.Spinner")
    def test_vm_launch_no_instance_id_parse(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": True, "stdout": "not json", "stderr": "",
            "error_category": None, "remedy": "",
        }

        result = aws_provisioner.provision_compute_vm()

        assert result["ok"] is True
        assert result["instance_id"] is None


class TestProvisionAuroraPostgres:
    @patch("setup.aws_provisioner.Spinner")
    def test_aurora_missing_password(self, mock_spinner_cls, aws_provisioner):
        result = aws_provisioner.provision_aurora_postgres(master_password="")

        assert result["ok"] is False
        assert result["message"] == "missing_password"

    @patch("setup.aws_provisioner.Spinner")
    def test_aurora_create_success(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({"DBCluster": {"Endpoint": "my-cluster.cluster-xxx.rds.amazonaws.com", "Port": 5432}}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": "{}",
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = aws_provisioner.provision_aurora_postgres(
            master_password="secret123", region="us-east-1"
        )

        assert result["ok"] is True
        assert result["endpoint"] == "my-cluster.cluster-xxx.rds.amazonaws.com"
        assert result["port"] == 5432

    @patch("setup.aws_provisioner.Spinner")
    def test_aurora_already_exists_reuse(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": False, "stderr": "DBClusterAlreadyExists",
                "error_category": ErrorCategory.ALREADY_EXISTS, "remedy": "reuse",
            },
            {
                "ok": True,
                "stdout": json.dumps({"DBClusters": [{"Endpoint": "existing.cluster-xxx.rds.amazonaws.com", "Port": 5432}]}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]
        mock_cli.prompt_choice.return_value = "Reuse existing aurora cluster"

        result = aws_provisioner.provision_aurora_postgres(
            master_password="secret123", region="us-east-1"
        )

        assert result["ok"] is True
        assert result["endpoint"] == "existing.cluster-xxx.rds.amazonaws.com"

    @patch("setup.aws_provisioner.Spinner")
    def test_aurora_unauthorized(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "UnauthorizedOperation",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = aws_provisioner.provision_aurora_postgres(master_password="pw", region="us-east-1")

        assert result["ok"] is False
        assert result["message"] == "unauthorized"

    @patch("setup.aws_provisioner.Spinner")
    def test_aurora_primary_instance_already_exists(self, mock_spinner_cls, aws_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({"DBCluster": {"Endpoint": "ep.rds.amazonaws.com", "Port": 5432}}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": False,
                "stdout": "",
                "stderr": "DB Instance already exists",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = aws_provisioner.provision_aurora_postgres(master_password="pw", region="us-east-1")

        assert result["ok"] is True


class TestDescribeAuroraCluster:
    def test_describe_success(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"DBClusters": [{"Endpoint": "ep.rds.amazonaws.com", "Port": 5432}]}),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = aws_provisioner._describe_aurora_cluster("my-cluster", "us-east-1")

        assert result["ok"] is True
        assert result["endpoint"] == "ep.rds.amazonaws.com"

    def test_describe_empty_clusters(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"DBClusters": []}),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = aws_provisioner._describe_aurora_cluster("missing-cluster", "us-east-1")

        assert result["ok"] is False
        assert result["message"] == "describe_failed"
        assert result["endpoint"] is None

    def test_describe_failure(self, aws_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": False,
            "stdout": "",
            "stderr": "error",
            "error_category": ErrorCategory.UNKNOWN,
            "remedy": "",
        }

        result = aws_provisioner._describe_aurora_cluster("my-cluster", "us-east-1")

        assert result["ok"] is False
        assert result["endpoint"] is None


class TestAWSProvision:
    @patch("setup.aws_provisioner.Spinner")
    def test_full_provision_success(self, mock_spinner_cls, aws_provisioner, mock_cli, mock_env_writer,
                                    sample_storage_new, sample_compute_vm, sample_dw_empty):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}
        mock_cli.prompt_choice.return_value = None
        mock_env_writer.read_value.return_value = "us-east-1"

        result = aws_provisioner.provision(sample_storage_new, sample_compute_vm, sample_dw_empty)

        assert result["ok"] is True
        assert "bucket" in result
        assert "layers" in result
        assert "vm" in result

    @patch("setup.aws_provisioner.Spinner")
    def test_provision_existing_storage(self, mock_spinner_cls, aws_provisioner, mock_cli, mock_env_writer,
                                        sample_storage_existing, sample_compute_local, sample_dw_empty):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}
        mock_env_writer.read_value.return_value = "us-east-1"

        result = aws_provisioner.provision(sample_storage_existing, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["bucket"]["message"] == "existing"
        assert result["vm"]["message"] == "skipped"

    @patch("setup.aws_provisioner.Spinner")
    def test_provision_with_managed_cloud_aurora(self, mock_spinner_cls, aws_provisioner, mock_cli, mock_env_writer,
                                                  sample_storage_existing, sample_compute_local, sample_dw_managed_provision):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"DBCluster": {"Endpoint": "aurora.example.com", "Port": 5432}}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""},
        ]
        mock_env_writer.read_value.return_value = "us-east-1"

        result = aws_provisioner.provision(sample_storage_existing, sample_compute_local, sample_dw_managed_provision)

        assert result["ok"] is True
        assert result.get("aurora", {}).get("ok") is True
        mock_env_writer.update_metadata.assert_called_once()
