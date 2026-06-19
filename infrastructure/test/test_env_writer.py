import os
import stat
from unittest.mock import patch

import pytest

from setup.env_writer import EnvWriter

_DEFAULT_RG = EnvWriter._DEFAULT_AZURE_RESOURCE_GROUP
_DEFAULT_LOC = EnvWriter._DEFAULT_AZURE_LOCATION


class TestEnvWriterWrite:
    def test_write_aws_no_dw(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": False, "name": "my-bucket", "layers": ["bronze", "silver"]}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "CLOUD_PROVIDER=AWS" in content
        assert "AWS_S3_BUCKET=my-bucket" in content
        assert "STORAGE_LAYERS=bronze,silver" in content
        assert "COMPUTE_TYPE=Local Machine" in content
        assert "INSTALL_LOCAL_POSTGRES" not in content
        assert "DW_HOST" not in content

    def test_write_azure_no_dw(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": False, "name": "myaccount", "layers": []}
        compute = {"compute": "Serverless/PaaS", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write(
            "Azure",
            storage,
            compute,
            dw,
            setup_name="acme-prod",
            setup_dir="infrastructure/setups/acme-prod",
        )

        content = env_path.read_text()
        assert "CLOUD_PROVIDER=Azure" in content
        assert "SETUP_NAME=acme-prod" in content
        assert "QUICKELT_SETUP_NAME=acme-prod" in content
        assert "QUICKELT_SETUP_DIR=infrastructure/setups/acme-prod" in content
        assert "AZURE_STORAGE_ACCOUNT=myaccount" in content
        assert f"AZURE_RESOURCE_GROUP={_DEFAULT_RG}" in content
        assert f"AZURE_LOCATION={_DEFAULT_LOC}" in content
        assert "STORAGE_LAYERS=" in content

    def test_write_with_local_vm_dw(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "my-bucket", "layers": []}
        compute = {"compute": "Dedicated VM", "bootstrap_vm": True}
        dw = {
            "gold_external_db": True,
            "pg_strategy": "local_vm",
            "install_local_postgres": True,
            "dw_host": "localhost",
            "dw_port": "5432",
            "dw_database": "quickelt_db",
            "dw_username": "quickelt",
            "dw_password": "secret123",
            "managed_cloud_choice": None,
        }

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "INSTALL_LOCAL_POSTGRES=true" in content
        assert "DW_HOST=localhost" in content
        assert "DW_PORT=5432" in content
        assert "DW_DATABASE=quickelt_db" in content
        assert "DW_USERNAME=quickelt" in content
        assert "DW_PASSWORD=secret123" in content
        assert "PG_STRATEGY=local_vm" in content

        file_stat = os.stat(env_path)
        assert stat.S_IMODE(file_stat.st_mode) == 0o600

    def test_write_with_managed_cloud_dw(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "my-bucket", "layers": []}
        compute = {"compute": "Dedicated VM", "bootstrap_vm": True}
        dw = {
            "gold_external_db": True,
            "pg_strategy": "managed_cloud",
            "install_local_postgres": False,
            "dw_host": "",
            "dw_port": "5432",
            "dw_database": "quickelt_db",
            "dw_username": "quickelt",
            "dw_password": "managed_pw",
            "managed_cloud_choice": "provision_new",
        }

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "PG_STRATEGY=managed_cloud" in content
        assert "MANAGED_CLOUD_CHOICE=provision_new" in content
        assert "INSTALL_LOCAL_POSTGRES=false" in content

        file_stat = os.stat(env_path)
        assert stat.S_IMODE(file_stat.st_mode) == 0o600

    def test_write_no_dw_no_chmod_restriction(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "my-bucket", "layers": []}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write("AWS", storage, compute, dw)

        file_stat = os.stat(env_path)
        assert stat.S_IMODE(file_stat.st_mode) != 0o600

    def test_write_oserror_exits(self, env_path):
        env = EnvWriter(env_path)
        storage = {"existing": False, "name": "b", "layers": []}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(SystemExit) as exc_info:
                env.write("AWS", storage, compute, dw)
            assert exc_info.value.code == 1


class TestEnvWriterUpdateMetadata:
    def test_update_metadata_existing_keys(self, env_path):
        env_path.write_text("CLOUD_PROVIDER=AWS\nAWS_S3_BUCKET=my-bucket\n")

        env = EnvWriter(env_path)
        env.update_metadata({"AWS_S3_BUCKET": "new-bucket", "NEW_KEY": "new_value"})

        content = env_path.read_text()
        assert "AWS_S3_BUCKET=new-bucket" in content
        assert "NEW_KEY=new_value" in content

    def test_update_metadata_new_file(self, env_path):
        env_path.unlink(missing_ok=True)

        env = EnvWriter(env_path)
        env.update_metadata({"FOO": "bar"})

        assert env_path.exists()
        content = env_path.read_text()
        assert "FOO=bar" in content

    def test_update_metadata_preserves_comments(self, env_path):
        env_path.write_text("# comment\nCLOUD_PROVIDER=AWS\n")

        env = EnvWriter(env_path)
        env.update_metadata({"CLOUD_PROVIDER": "Azure"})

        content = env_path.read_text()
        assert "# comment" in content
        assert "CLOUD_PROVIDER=Azure" in content

    def test_update_metadata_oserror_handled(self, env_path):
        env_path.write_text("KEY=val\n")

        env = EnvWriter(env_path)
        with patch("builtins.open", side_effect=OSError("read only")):
            env.update_metadata({"KEY": "new"})
        assert not env_path.read_text().__contains__("KEY=new")


class TestEnvWriterReadValue:
    def test_read_value_existing(self, env_path):
        env_path.write_text("CLOUD_PROVIDER=AWS\nAWS_S3_BUCKET=my-bucket\n")

        env = EnvWriter(env_path)
        assert env.read_value("CLOUD_PROVIDER") == "AWS"
        assert env.read_value("AWS_S3_BUCKET") == "my-bucket"

    def test_read_value_missing(self, env_path):
        env_path.write_text("CLOUD_PROVIDER=AWS\n")

        env = EnvWriter(env_path)
        assert env.read_value("NONEXISTENT") is None

    def test_read_value_empty_file(self, env_path):
        env = EnvWriter(env_path)
        assert env.read_value("ANY") is None

    def test_read_value_no_file(self, env_path):
        env_path.unlink(missing_ok=True)

        env = EnvWriter(env_path)
        assert env.read_value("ANY") is None

    def test_read_value_with_equals_in_value(self, env_path):
        env_path.write_text("CONNECTION_STRING=host=db port=5432\n")

        env = EnvWriter(env_path)
        assert env.read_value("CONNECTION_STRING") == "host=db port=5432"


class TestEnvWriterRestrictPermissions:
    def test_restrict_permissions(self, env_path):
        env_path.touch()
        env = EnvWriter(env_path)
        env._restrict_permissions()

        file_stat = os.stat(env_path)
        assert stat.S_IMODE(file_stat.st_mode) == 0o600

    def test_restrict_permissions_oserror_handled(self, env_path):
        env = EnvWriter(env_path)
        with patch("os.chmod", side_effect=OSError("nope")):
            env._restrict_permissions()


class TestEnvWriterWritePreservesExisting:
    def test_preserves_user_keys(self, env_path):
        env_path.write_text("MY_CUSTOM_KEY=custom_value\nCLOUD_PROVIDER=OldValue\n")

        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "my-bucket", "layers": []}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "MY_CUSTOM_KEY=custom_value" in content
        assert "CLOUD_PROVIDER=AWS" in content
        assert "AWS_S3_BUCKET=my-bucket" in content

    def test_overrides_existing_wizard_keys(self, env_path):
        env_path.write_text("CLOUD_PROVIDER=OldCloud\nAWS_S3_BUCKET=old-bucket\n")

        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "new-bucket", "layers": []}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "CLOUD_PROVIDER=AWS" in content
        assert "AWS_S3_BUCKET=new-bucket" in content
        assert "OldCloud" not in content
        assert "old-bucket" not in content

    def test_preserves_comments(self, env_path):
        env_path.write_text("# My custom comment\nMY_VAR=123\n")

        env = EnvWriter(env_path)
        storage = {"existing": True, "name": "b", "layers": []}
        compute = {"compute": "Local Machine", "bootstrap_vm": False}
        dw = {"gold_external_db": False}

        env.write("AWS", storage, compute, dw)

        content = env_path.read_text()
        assert "# My custom comment" in content
        assert "MY_VAR=123" in content


class TestLoadSetupConfig:
    def test_load_azure_config(self, env_path):
        env_path.write_text(
            "CLOUD_PROVIDER=Azure\n"
            "AZURE_STORAGE_ACCOUNT=company-lake\n"
            "STORAGE_LAYERS=bronze,silver\n"
            "COMPUTE_TYPE=Serverless/PaaS\n"
            "BOOTSTRAP_VM=false\n"
            "PG_STRATEGY=managed_cloud\n"
            "MANAGED_CLOUD_CHOICE=provision_new\n"
            "DW_USERNAME=quickelt\n"
            "DW_PASSWORD=secret\n"
        )

        env = EnvWriter(env_path)
        cloud, storage, compute, dw = env.load_setup_config()

        assert cloud == "Azure"
        assert storage["name"] == "company-lake"
        assert storage["layers"] == ["bronze", "silver"]
        assert compute["compute"] == "Serverless/PaaS"
        assert dw["gold_external_db"] is True
        assert dw["pg_strategy"] == "managed_cloud"
        assert dw["managed_cloud_choice"] == "provision_new"
        assert dw["dw_password"] == "secret"
