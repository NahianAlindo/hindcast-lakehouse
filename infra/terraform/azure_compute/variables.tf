variable "subscription_id" {
  description = "The single Azure for Students subscription this project uses"
  type        = string
  default     = "29331902-efb2-452a-a964-fafe163f4321"
}

variable "location" {
  description = "westus2 -- the only region where this subscription actually had free-tier VM capacity today (eastus was capacity-restricted for every size tried). The persistent disk needs to move here from azure_data's eastus to attach (tracked separately, not yet done)."
  type        = string
  default     = "westus2"
}

variable "project" {
  type    = string
  default = "hindcast"
}

variable "vm_size" {
  description = "Standard_B2ats_v2 -- confirmed available and free-services-eligible in westus2 via the Portal's live size picker, after eastus rejected every size tried (B2s, B2ms, B2ats_v2, B1s) with capacity restrictions."
  type        = string
  default     = "Standard_B2ats_v2"
}

variable "admin_username" {
  type    = string
  default = "hindcast"
}

variable "ssh_public_key" {
  description = "Public half of the ed25519 key generated for this project; the matching private key stays local, never in git"
  type        = string
  default     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIL3gOEo3okMZhAqASKtYLgH3+JdQMMedhzxELwImpoU8 nahian.rifaat@ontariotechu.net"
}

variable "home_ip" {
  description = "Current dev-machine public IP -- the only address the NSG allows SSH from"
  type        = string
  default     = "70.27.60.193"
}
