variable "subscription_id" {
  description = "The single Azure for Students subscription this project uses (deliberately not the second available one -- see CLAUDE.md)"
  type        = string
  default     = "29331902-efb2-452a-a964-fafe163f4321"
}

variable "location" {
  description = "Azure region. Azure for Students subscriptions restrict which regions certain resource types can deploy to via an Azure Policy ('best available regions') -- canadacentral was rejected for storage/Key Vault with a 403. eastus is confirmed working for storage/Key Vault."
  type        = string
  default     = "eastus"
}

variable "project" {
  type    = string
  default = "hindcast"
}
