# We strongly recommend using the required_providers block to set the
# Azure Provider source and version being used
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=4.1.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  resource_provider_registrations = "none" # This is only required when the User, Service Principal, or Identity running Terraform lacks the permissions to register Azure Resource Providers.
  features {}
  subscription_id = "ff5d6fdb-7445-4e2c-9ed7-274fb3cf49ce"
}

# Create a resource group
# resource "azurerm_resource_group" "example" {
#   name     = "vikash-resource"
#   location = "West Europe"
# }

resource "azurerm_virtual_network" "example" {
  name                = "Vikash-network-New"
  resource_group_name = "Sigmoid"
  location            = var.location
  address_space       = ["10.0.0.0/16"]
}

variable location {
  type        = string
  # default     = "East US"
}

