Current barriers:

\- CloudShell cannot create an environment.

\- IAM security credentials page returns AccessDenied.

\- The console reports missing permission for iam:GetUser.

\- Access key creation is denied.



Impact:

\- Bedrock cannot be tested from local PowerShell because AWS CLI requires an access key id and secret access key.



Needed:

\- Enable CloudShell, or

\- provide an access key pair, or

\- grant the minimum permissions needed for Bedrock CLI.

