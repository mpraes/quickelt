import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from setup.azure_provisioner import AzureProvisioner
from setup.cli_executor import ErrorCategory

_TEST_LOGGER = logging.getLogger("test.azure_provisioner")


@pytest.fixture
def azure_provisioner(mock_cli, mock_env_writer):
    return AzureProvisioner(cli=mock_cli, env=mock_env_writer, logger=_TEST_LOGGER)


class TestParseJson:
    def test_valid_json(self, azure_provisioner):
        assert azure_provisioner._parse_json('{"key": "val"}') == {"key": "val"}

    def test_invalid_json(self, azure_provisioner):
        assert azure_provisioner._parse_json("not json") is None

    def test_empty_string(self, azure_provisioner):
        assert azure_provisioner._parse_json("") is None

    def test_none_input(self, azure_provisioner):
        assert azure_provisioner._parse_json(None) is None


class TestGetSubscriptionLocation:
    def test_location_from_cli(self, azure_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"location": "westeurope"}),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        assert azure_provisioner._get_subscription_location() == "westeurope"

    def test_location_from_env_fallback(self, azure_provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": "", "stderr": "err"}

        with patch.dict("os.environ", {"AZURE_LOCATION": "northeurope"}):
            assert azure_provisioner._get_subscription_location() == "northeurope"

    def test_location_default(self, azure_provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": "", "stderr": "err"}

        assert azure_provisioner._get_subscription_location() == "eastus"


class TestEnsureResourceGroup:
    @patch("setup.azure_provisioner.Spinner")
    def test_rg_already_exists(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": True, "stdout": "true", "stderr": "",
            "error_category": None, "remedy": "",
        }

        result = azure_provisioner._ensure_resource_group("my-rg", "eastus")

        assert result["ok"] is True
        assert result["message"] == "already_exists"

    @patch("setup.azure_provisioner.Spinner")
    def test_rg_create_new(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True, "stdout": "false", "stderr": "",
                "error_category": None, "remedy": "",
            },
            {
                "ok": True, "stdout": "{}", "stderr": "",
                "error_category": None, "remedy": "",
            },
        ]

        result = azure_provisioner._ensure_resource_group("my-rg", "eastus")

        assert result["ok"] is True
        assert result["message"] == "created"

    @patch("setup.azure_provisioner.Spinner")
    def test_rg_auth_expired(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "ExpiredAuthenticationToken",
            "error_category": ErrorCategory.AUTH_EXPIRED, "remedy": "az login",
        }

        result = azure_provisioner._ensure_resource_group("my-rg", "eastus")

        assert result["ok"] is False
        assert result["message"] == "auth_expired"

    @patch("setup.azure_provisioner.Spinner")
    def test_rg_unauthorized(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "AuthorizationFailed",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = azure_provisioner._ensure_resource_group("my-rg", "eastus")

        assert result["ok"] is False
        assert result["message"] == "unauthorized"


class TestCreateAzureLake:
    @patch("setup.azure_provisioner.Spinner")
    def test_create_lake_success(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "true", "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": True,
                "stdout": json.dumps({"primaryEndpoints": {"dfs": "https://acct.dfs.core.windows.net"}}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {"ok": True, "stdout": '"mykey123"', "stderr": "", "error_category": None, "remedy": ""},
        ]

        result = azure_provisioner.create_azure_lake("myaccount", "my-rg")

        assert result["ok"] is True
        assert result["message"] == "created"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_lake_already_exists_reuse(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "true", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": False, "stderr": "StorageAccountAlreadyExists",
                "error_category": ErrorCategory.ALREADY_EXISTS, "remedy": "reuse",
            },
            {
                "ok": True,
                "stdout": json.dumps({"primaryEndpoints": {"blob": "https://acct.blob.core.windows.net"}}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {"ok": True, "stdout": '"keyval"', "stderr": "", "error_category": None, "remedy": ""},
        ]
        mock_cli.prompt_choice.return_value = "Reuse existing storage account"

        result = azure_provisioner.create_azure_lake("myaccount", "my-rg")

        assert result["ok"] is True
        assert result["message"] == "already_exists"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_lake_auth_expired(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "true", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": False, "stderr": "ExpiredAuthenticationToken",
                "error_category": ErrorCategory.AUTH_EXPIRED, "remedy": "az login",
            },
        ]

        result = azure_provisioner.create_azure_lake("myaccount", "my-rg")

        assert result["ok"] is False
        assert result["message"] == "auth_expired"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_lake_invalid_name(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "true", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": False, "stderr": "not a valid storage account name",
                "error_category": ErrorCategory.INVALID_NAME, "remedy": "fix name",
            },
        ]

        result = azure_provisioner.create_azure_lake("BAD_NAME", "my-rg")

        assert result["ok"] is False


class TestStructureAzureLayers:
    @patch("setup.azure_provisioner.Spinner")
    def test_all_layers_created(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""}

        result = azure_provisioner.structure_azure_layers("myaccount", ["bronze", "silver"])

        assert result["ok"] is True
        assert "bronze" in result["created"]
        assert "silver" in result["created"]

    @patch("setup.azure_provisioner.Spinner")
    def test_container_creation_fails(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "AuthorizationFailed",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = azure_provisioner.structure_azure_layers("myaccount", ["bronze"])

        assert result["ok"] is False

    @patch("setup.azure_provisioner.Spinner")
    def test_layer_directory_fallback_blob_upload(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": False, "stdout": "", "stderr": "HNS error",
                "error_category": ErrorCategory.UNKNOWN, "remedy": "err",
            },
            {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""},
        ]

        result = azure_provisioner.structure_azure_layers("myaccount", ["bronze"])

        assert result["ok"] is True
        assert "bronze" in result["created"]


class TestProvisionComputeVm:
    @patch("setup.azure_provisioner.Spinner")
    def test_vm_success(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({
                    "id": "/subscriptions/xxx/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/quickelt-vm",
                    "vmId": "vm-123",
                    "publicIps": "1.2.3.4",
                    "privateIps": "10.0.0.1",
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": "",
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = azure_provisioner.provision_compute_vm("my-rg", bootstrap=True)

        assert result["ok"] is True
        assert result["vm_id"] == "vm-123"
        assert result["public_ip"] == "1.2.3.4"
        assert result["private_ip"] == "10.0.0.1"

    @patch("setup.azure_provisioner.Spinner")
    def test_vm_with_local_postgres(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({
                    "id": "/subscriptions/xxx/resGroups/my-rg/providers/Microsoft.Compute/virtualMachines/quickelt-vm",
                    "vmId": "vm-456",
                    "publicIps": "5.6.7.8",
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": "",
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = azure_provisioner.provision_compute_vm(
            "my-rg", bootstrap=True, install_local_postgres=True, dw_password="mypass"
        )

        assert result["ok"] is True

    @patch("setup.azure_provisioner.Spinner")
    def test_vm_unauthorized(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "AuthorizationFailed",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = azure_provisioner.provision_compute_vm("my-rg")

        assert result["ok"] is False
        assert result["message"] == "unauthorized"

    @patch("setup.azure_provisioner.Spinner")
    def test_vm_auth_expired(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "ExpiredAuthenticationToken",
            "error_category": ErrorCategory.AUTH_EXPIRED, "remedy": "az login",
        }

        result = azure_provisioner.provision_compute_vm("my-rg")

        assert result["ok"] is False
        assert result["message"] == "auth_expired"

    @patch("setup.azure_provisioner.Spinner")
    def test_vm_no_public_ip_queries(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({
                    "id": "/subscriptions/xxx/resGroups/my-rg/providers/Microsoft.Compute/virtualMachines/quickelt-vm",
                    "vmId": "vm-789",
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": '"10.0.0.99"',
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": "",
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = azure_provisioner.provision_compute_vm("my-rg", bootstrap=True)

        assert result["ok"] is True
        assert result["public_ip"] == "10.0.0.99"


class TestProvisionAzurePostgres:
    @patch("setup.azure_provisioner.Spinner")
    def test_missing_password(self, mock_spinner_cls, azure_provisioner):
        result = azure_provisioner.provision_azure_postgres("my-rg", admin_password="")

        assert result["ok"] is False
        assert result["message"] == "missing_password"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_success(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({
                "fullyQualifiedDomainName": "myserver.postgres.database.azure.com",
                "port": 5432,
            }),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = azure_provisioner.provision_azure_postgres(
            "my-rg", admin_password="pw123", location="eastus"
        )

        assert result["ok"] is True
        assert result["fqdn"] == "myserver.postgres.database.azure.com"
        assert result["port"] == 5432

    @patch("setup.azure_provisioner.Spinner")
    def test_already_exists_reuse(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": False, "stderr": "already exists",
                "error_category": ErrorCategory.ALREADY_EXISTS, "remedy": "reuse",
            },
            {
                "ok": True,
                "stdout": json.dumps({
                    "fullyQualifiedDomainName": "existing.postgres.database.azure.com",
                    "port": 5432,
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]
        mock_cli.prompt_choice.return_value = "Reuse existing postgresql server"

        result = azure_provisioner.provision_azure_postgres("my-rg", admin_password="pw", location="eastus")

        assert result["ok"] is True
        assert result["fqdn"] == "existing.postgres.database.azure.com"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_unauthorized(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {
            "ok": False, "stderr": "AuthorizationFailed",
            "error_category": ErrorCategory.UNAUTHORIZED, "remedy": "denied",
        }

        result = azure_provisioner.provision_azure_postgres("my-rg", admin_password="pw", location="eastus")

        assert result["ok"] is False
        assert result["message"] == "unauthorized"

    @patch("setup.azure_provisioner.Spinner")
    def test_create_fqdn_fallback_to_describe(self, mock_spinner_cls, azure_provisioner, mock_cli):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.side_effect = [
            {
                "ok": True,
                "stdout": json.dumps({"someOtherKey": "val"}),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {
                "ok": True,
                "stdout": json.dumps({
                    "fullyQualifiedDomainName": "fallback.postgres.database.azure.com",
                    "port": 5432,
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]

        result = azure_provisioner.provision_azure_postgres("my-rg", admin_password="pw", location="eastus")

        assert result["ok"] is True
        assert result["fqdn"] == "fallback.postgres.database.azure.com"


class TestDescribeAzurePostgres:
    def test_describe_success(self, azure_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({
                "fullyQualifiedDomainName": "srv.postgres.database.azure.com",
                "port": 5432,
                "location": "eastus",
            }),
            "stderr": "",
            "error_category": None,
            "remedy": "",
        }

        result = azure_provisioner._describe_azure_postgres("srv", "my-rg")

        assert result["ok"] is True
        assert result["fqdn"] == "srv.postgres.database.azure.com"
        assert result["location"] == "eastus"

    def test_describe_failure(self, azure_provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": False, "stdout": "", "stderr": "not found",
            "error_category": ErrorCategory.NOT_FOUND, "remedy": "",
        }

        result = azure_provisioner._describe_azure_postgres("missing", "my-rg")

        assert result["ok"] is False
        assert result["message"] == "describe_failed"
        assert result["fqdn"] is None


class TestAzureProvision:
    @patch("setup.azure_provisioner.Spinner")
    def test_full_provision_success(self, mock_spinner_cls, azure_provisioner, mock_cli, mock_env_writer,
                                     sample_storage_new, sample_compute_vm, sample_dw_empty):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}
        mock_env_writer.read_value.return_value = "quickelt-rg"

        result = azure_provisioner.provision(sample_storage_new, sample_compute_vm, sample_dw_empty)

        assert result["ok"] is True
        assert "lake" in result
        assert "layers" in result
        assert "vm" in result

    @patch("setup.azure_provisioner.Spinner")
    def test_provision_existing_storage(self, mock_spinner_cls, azure_provisioner, mock_cli, mock_env_writer,
                                         sample_storage_existing, sample_compute_local, sample_dw_empty):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}
        mock_env_writer.read_value.return_value = "quickelt-rg"

        result = azure_provisioner.provision(sample_storage_existing, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["lake"]["message"] == "existing"
        assert result["vm"]["message"] == "skipped"

    @patch("setup.azure_provisioner.Spinner")
    def test_provision_with_managed_cloud_postgres(self, mock_spinner_cls, azure_provisioner, mock_cli, mock_env_writer,
                                                     sample_storage_existing, sample_compute_local, sample_dw_managed_provision):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        sample_storage_existing["layers"] = []
        mock_cli.execute.side_effect = [
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": "true", "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": True,
                "stdout": json.dumps({
                    "primaryEndpoints": {"dfs": "https://acct.dfs.core.windows.net"},
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
            {"ok": True, "stdout": '"keyval"', "stderr": "", "error_category": None, "remedy": ""},
            {"ok": True, "stdout": json.dumps({"location": "eastus"}), "stderr": "", "error_category": None, "remedy": ""},
            {
                "ok": True,
                "stdout": json.dumps({
                    "fullyQualifiedDomainName": "myserver.postgres.database.azure.com",
                    "port": 5432,
                }),
                "stderr": "",
                "error_category": None,
                "remedy": "",
            },
        ]
        mock_env_writer.read_value.return_value = "quickelt-rg"

        result = azure_provisioner.provision(sample_storage_existing, sample_compute_local, sample_dw_managed_provision)

        assert result["ok"] is True
        assert result.get("postgres", {}).get("ok") is True
        mock_env_writer.update_metadata.assert_called_once()

    @patch("setup.azure_provisioner.Spinner")
    def test_provision_no_layers(self, mock_spinner_cls, azure_provisioner, mock_cli, mock_env_writer,
                                  sample_storage_existing, sample_compute_local, sample_dw_empty):
        spinner = MagicMock()
        spinner.start.return_value = spinner
        mock_spinner_cls.return_value = spinner

        sample_storage_existing["layers"] = []
        mock_cli.execute.return_value = {"ok": True, "stdout": "{}", "stderr": "", "error_category": None, "remedy": ""}
        mock_env_writer.read_value.return_value = "quickelt-rg"

        result = azure_provisioner.provision(sample_storage_existing, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["layers"]["created"] == []
        assert result["layers"]["failed"] == []
