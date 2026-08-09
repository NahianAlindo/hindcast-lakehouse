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
  description = "Standard_B4als_v2 (4 vCPU, 8GB RAM, x64) -- Phase 4 resize up from Standard_B2als_v2 (2 vCPU, 4GB). Running bronze_to_silver_spark's three independent flatten jobs in true parallel (current/air_quality/forecast, no dependency edges between them) oversubscribed 2 vCPUs badly enough to hang the guest OS itself -- SSH couldn't complete its banner exchange and even Azure's out-of-band VM-agent run-command channel stalled for minutes, while the VM stayed 'running' at the control-plane level the whole time (confirming in-guest CPU starvation, not an Azure-side or crashed-VM problem). Same x64 architecture as the prior size, so another safe in-place resize -- no VM recreation, no data risk, and issuing it doesn't require the hung guest OS to cooperate (resize is a control-plane operation)."
  type        = string
  default     = "Standard_B4als_v2"
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
