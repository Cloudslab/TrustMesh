# TrustMesh

TrustMesh is a blockchain-enabled distributed computing framework designed for trustless heterogeneous IoT environments. It implements a novel three-layer architecture combining permissioned blockchain technology with a multi-phase Practical Byzantine Fault Tolerance (PBFT) consensus based protocol for scheduling decisions.

## Key Features

- **Three-Layer Architecture**:
   - Network Management Layer for system setup and application management
   - Computation Layer with blockchain-based consensus
   - Perception Layer for IoT device integration

- **Byzantine Fault Tolerance**: Maintains security and consensus while supporting non-deterministic scheduling algorithms

- **Heterogeneous Environment Support**: Designed to work across varied IoT devices and computing resources

- **Secure Workflow Management**: Implements complex workflow pipelines with dependency management

- **Efficient Resource Allocation**: Uses Least-Connected Dynamic Weighted Round Robin (LCDWRR) scheduling

## Code & Resource Access

The TrustMesh codebase is available at https://github.com/Cloudslab/TrustMesh. Please use the **artifact-evaluation** branch to proceed with the review.

As specified in the `INSTALL.md`, a minimum of 6 nodes are required for testing the framework.

> **Note for Reviewers**: If you require access to a pre-configured testing environment, please contact mrangwala@student.unimelb.edu.au. We can provide temporary access to a fully configured control-plane instance for evaluation purposes. These instances are maintained for 24-hour testing periods, after which a new instance can be requested if needed.

## Reproducing Results
### Performance

Based on our experimental evaluation with a 21-node testbed and the cold-chain application:
- Request Round Trip (RRT) time: 33.54 - 36.34 seconds
- Framework Overhead: 3.25 - 4.19 seconds
- Scales effectively up to 16 computation nodes

> **Note:** Please keep in mind that we used several versions of the cold-chain anomaly detection application with varying computational requirements to attain these results. Only one of those variations is included in this project so your Request Roundtrip Time (RRT) may vary. However, you should find that the Framework Overhead (FO) remains the same. Feel free to update `sample-apps/cold-chain-monitoring/task2_detect-anomalies/process.py` with a new default value for `n_samples` to test with varying computational demand.

### Resource Monitoring

Figure 4 in the paper demonstrates resource usage during the framework lifecycle. To generate the data used for these graphs, you may run the `observation-metrics/resource_monitor.py` script in a separate terminal of the k3s server instance while performing the `Basic Usage Example` steps from the `INSTALL.md` file. Terminate the script once the workflow is done. The resource usage results will be stored in the same directory as the resource monitoring script. 

## License

This project is licensed under the GNU General Public License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use TrustMesh in your research, please cite:
```
@misc{rangwala2024trustmeshblockchainenabledtrusteddistributed,
      title={TrustMesh: A Blockchain-Enabled Trusted Distributed Computing Framework for Open Heterogeneous IoT Environments}, 
      author={Murtaza Rangwala and Rajkumar Buyya},
      year={2024},
      eprint={2411.13039},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2411.13039}, 
}
```

## Authors

- Murtaza Rangwala - [Email](mailto:mrangwala@student.unimelb.edu.au)
- Rajkumar Buyya - [Email](mailto:rbuyya@unimelb.edu.au)

Cloud Computing and Distributed Systems (CLOUDS) Laboratory  
School of Computing and Information Systems  
The University of Melbourne, Australia
