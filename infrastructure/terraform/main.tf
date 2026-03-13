# =============================================================================
# Azure Infrastructure — Terraform
# =============================================================================
# Provisions all Azure resources required for the Lakehouse:
#   - Resource Group
#   - Azure Data Lake Storage Gen2 (with Bronze/Silver/Gold containers)
#   - Azure Databricks Workspace
#   - Azure Key Vault (for secrets management)
#   - Azure Data Factory (for orchestration)
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }

  backend "azurerm" {
    resource_group_name  = "tfstate-rg"
    storage_account_name = "tfstatelakehouse"
    container_name       = "tfstate"
    key                  = "lakehouse.terraform.tfstate"
  }
}

# ── Variables ────────────────────────────────────────────────────────────

variable "environment" {
  type        = string
  default     = "dev"
  description = "Environment name (dev, staging, prod)"
}

variable "location" {
  type        = string
  default     = "northeurope"
  description = "Azure region for all resources"
}

variable "project_name" {
  type        = string
  default     = "lakehouse"
  description = "Project name used in resource naming"
}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
}

# ── Provider Configuration ───────────────────────────────────────────────

provider "azurerm" {
  features {}
}

# ── Resource Group ───────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.resource_prefix}"
  location = var.location
  tags     = local.tags
}

# ── Azure Data Lake Storage Gen2 ─────────────────────────────────────────

resource "azurerm_storage_account" "datalake" {
  name                     = replace("st${local.resource_prefix}", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Enables Data Lake Storage Gen2

  tags = local.tags
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Create directory structure for medallion architecture
resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_container.lakehouse.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_container.lakehouse.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_container.lakehouse.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

# ── Azure Databricks Workspace ───────────────────────────────────────────

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.resource_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"

  tags = local.tags
}

# ── Azure Key Vault ──────────────────────────────────────────────────────

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${local.resource_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  purge_protection_enabled = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge",
    ]
  }

  tags = local.tags
}

# Store storage account key in Key Vault
resource "azurerm_key_vault_secret" "storage_key" {
  name         = "storage-account-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
}

# ── Azure Data Factory ───────────────────────────────────────────────────

resource "azurerm_data_factory" "main" {
  name                = "adf-${local.resource_prefix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# ── Outputs ──────────────────────────────────────────────────────────────

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}
