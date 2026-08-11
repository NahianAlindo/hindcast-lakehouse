terraform {
  cloud {
    organization = "NahianAlindo"
    workspaces {
      name = "hindcast-azure-data"
    }
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    datadog = {
      source  = "DataDog/datadog"
      version = "~> 3.60"
    }
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id

  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}

# Phase 7: dashboard + monitors as code (docs/PLAN.md), not a manual
# UI-clickthrough deliverable that isn't tracked anywhere. Credentials read
# from DATADOG_API_KEY/DATADOG_APP_KEY env vars (the provider's own default
# names) -- same secrets already in Key Vault for the pipeline's own metric
# submission, set locally for `terraform apply`, not hardcoded here.
provider "datadog" {
  api_url = "https://api.us5.datadoghq.com/"
}
