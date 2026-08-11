variable "name" {
  description = "What everything here is called: the function, the repository, the role, the api."
  type        = string
  default     = "zou-demo"
}

variable "region" {
  description = "The region the bucket and the function live in. Put them in the same one or every store op pays a cross region round trip."
  type        = string
  default     = "eu-west-1"
}

variable "bucket" {
  description = "The bucket the whole database lives in. Bucket names are global, so this one has no default."
  type        = string
}

variable "prefix" {
  description = "The key prefix under the bucket. Everything zou writes is under it, and the role can reach nothing else."
  type        = string
  default     = "projects"
}

variable "ref" {
  description = "The project this deployment serves. It is decided here rather than per request, which is what makes /rest/v1/ spell the same as a hosted project's."
  type        = string
  default     = "demo"
}

variable "image_tag" {
  description = "The tag pushed to the repository this config creates."
  type        = string
  default     = "latest"
}

variable "architecture" {
  description = "arm64 or x86_64, and it has to be the one the image was built for."
  type        = string
  default     = "arm64"

  validation {
    condition     = contains(["arm64", "x86_64"], var.architecture)
    error_message = "architecture is arm64 or x86_64."
  }
}

variable "memory_mb" {
  description = "Memory, which on Lambda is also cpu: 1769 MB is one full vcpu and everything below it is a fraction of one. A postgres with a wal pusher and a page service in it wants more than a fraction."
  type        = number
  default     = 2048
}

variable "timeout_seconds" {
  description = "The invocation timeout. It has to cover the first attach of a project that has already been created, not the initdb, which is done from a laptop before the function ever runs."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  description = "How long the function's logs are kept. Never expiring is a bill that grows on its own."
  type        = number
  default     = 14
}
