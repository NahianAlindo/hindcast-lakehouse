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
  description = "Standard_B2als_v2 (4GB RAM, x64) -- Phase 3 resize up from Standard_B2ats_v2 (1024 MB, too tight for Airflow's own stated 4GB minimum). Same x64 architecture as the original size, so this was a safe in-place resize -- no VM recreation, no data risk. Worth knowing: `az vm list-skus` reports this size as NotAvailableForSubscription in westus2, but the resize succeeded anyway -- the same reported-vs-actual availability gap Phase 1 hit with the original VM creation. A first attempt at this resize wrongly targeted Standard_B2pls_v2 (ARM64) assuming it shared an architecture with the original size; Azure correctly rejected that with PropertyChangeNotAllowed since cross-architecture resize isn't possible -- worth remembering before assuming any other size's architecture from its name alone."
  type        = string
  default     = "Standard_B2als_v2"
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
  description = "Current dev-machine public IP -- the only address the NSG allows SSH from. ISP-assigned, rotates over time (was 70.27.60.193 as of Phase 1; SSH access silently breaks whenever it changes -- check `curl https://api.ipify.org` if SSH ever times out and this hasn't been updated to match)."
  type        = string
  default     = "70.31.77.21"
}
