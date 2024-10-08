# Task 2 - Metadata and Data Catalog

To securely store metadata, systems such as Apache Atlas or AWS Glue Data Catalog can be utilized. Specifically, AWS Glue Data Catalog offers seamless integration with other AWS services like crawlers, which can detect sensitive data, including Personally Identifiable Information (PII). 

A simple solution has been attached in this folder. The metadata is stored in a YAML format, providing clarity and ease of use, though it can also be stored in a database table for more structured access. Additionally, Role-Based Access Control (RBAC) permissions for users or groups is stored alongside the metadata. This would allow the allocation of permissions to control access to specific data objects based on roles.

A more advanced solution involves leveraging database features such as Google BigQuery’s tagging system, where fields can be tagged and role-based permissions applied. Depending on the user's role, they will be able to view either masked or unmasked data.

Similarly, in Amazon Redshift, data masking policies can be applied, and roles can be configured with masking or unmasking permissions. This ensures that only authorized users can view unmasked sensitive data. The process of assigning these permissions can be streamlined using IAM roles and managed via a CI/CD pipeline, which allows for more efficient configuration and deployment of permissions.