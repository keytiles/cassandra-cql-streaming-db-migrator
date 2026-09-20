In this codebase we follow our generic documentation guidelines.

Please read the Markdown document https://raw.githubusercontent.com/keytiles/ai-agents/refs/heads/main/docs/docs-rules-and-best-practices-v1.md and keep the rules it defines!

# Also consider the following

## Maven modules

In this repository we work with Maven with modules. Take a look into [pom.xml](../pom.xml) and see <modules>. Take the list of modules from there.

### Folders under modules

We also might have folders like `development-plans` or `docs` or `docs-user` under module subfolders separately and not necessarily only globally here in the parent module.


### CHANGELOG

The [CHANGELOG.md](../CHANGELOG.md) file however is global as the compiled .jar artifact is always created as a whole.

### Versioning policy

Versioning policy is Semantic Versioning - and also apply to the whole maven parent globally.
