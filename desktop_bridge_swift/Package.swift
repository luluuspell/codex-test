// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "LiandanluDesktopBridge",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .executable(
            name: "LiandanluDesktopBridge",
            targets: ["LiandanluDesktopBridge"]
        )
    ],
    targets: [
        .executableTarget(
            name: "LiandanluDesktopBridge",
            path: "Sources/LiandanluDesktopBridge"
        )
    ]
)
