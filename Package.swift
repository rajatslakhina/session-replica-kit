// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "session-replica-kit",
    // Only platforms CI actually builds are declared. Linux needs no
    // declaration; the demo app's CI builds for `generic/platform=iOS Simulator`.
    platforms: [
        .iOS(.v17),
        .macOS(.v14)
    ],
    products: [
        .library(name: "SessionReplica", targets: ["SessionReplica"]),
        .library(name: "SessionReplicaUI", targets: ["SessionReplicaUI"])
    ],
    targets: [
        .target(name: "SessionReplica"),
        .target(name: "SessionReplicaUI", dependencies: ["SessionReplica"]),
        .testTarget(name: "SessionReplicaTests", dependencies: ["SessionReplica"])
    ]
)
