# AMOS

![image](https://img.shields.io/badge/version-0.1.0-blue?style=for-the-badge) ![image](https://img.shields.io/badge/status-public--beta-yellow?style=for-the-badge) ![image](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)

---
[Overview](https://github.com/open-amos/) | [Starter](https://github.com/open-amos/starter) | **Core** | [Source Example](https://github.com/open-amos/source-example) | [Dashboard](https://github.com/open-amos/dashboard-example)

---

# AMOS Core 

AMOS Core is a dbt project providing a canonical model, curated marts and BI-ready metrics for private markets, to be used in conjunction with source connectors like [AMOS Source Example](../source-example). It is the central piece of the [AMOS data stack](../).

## Quick Start

Install and run [Amos Starter](https://github.com/open-amos/starter) (recommended) or add [AMOS Core](https://github.com/open-amos/core) as a dependency to your dbt project.

## Contents

- **Models**: Canonical model, curated marts and BI-ready metrics for private markets.
- **Documentation and tests** for the core models.

### Canonical Model

The canonical model is stored in the `models/core` directory. It is organized by entity and includes companies, funds, investors, instruments, transactions, facilities, loans, share_classes, commitments, opportunities, snapshots, and cashflows.

### Marts & Metrics (under development)

The marts are stored in the `models/marts` directory. Currently under development, they include metrics for fund performance, company performance, position performance, returns timeseries, and cashflows.

## Customization

The recommended way to customize AMOS Core is to create a new dbt project and add AMOS Core as a dependency. You can then create custom packages to add your own models and metrics, keeping AMOS Core separate from your custom code.

## Contributing

AMOS is open source and welcomes contributions. Report bugs, suggest features, add integration patterns, or submit pull requests.

## Licensing

This subproject is part of the AMOS public preview. Licensing terms will be finalized before version 1.0.
For now, the code is shared for evaluation and feedback only. Commercial or production use requires written permission from the maintainers.
