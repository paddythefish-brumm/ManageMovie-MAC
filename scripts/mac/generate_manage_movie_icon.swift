#!/usr/bin/env swift
import AppKit

let arguments = CommandLine.arguments
guard arguments.count >= 2 else {
    fputs("Usage: generate_manage_movie_icon.swift <output.icns>\n", stderr)
    exit(2)
}

let outputPath = arguments[1]
let fm = FileManager.default
let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("managemovie-icon-\(UUID().uuidString)")
let iconsetURL = tempRoot.appendingPathComponent("ManageMovie.iconset")

func drawIcon(size: CGFloat, scale: CGFloat) -> NSImage {
    let px = size * scale
    let image = NSImage(size: NSSize(width: px, height: px))
    image.lockFocus()
    guard let ctx = NSGraphicsContext.current?.cgContext else {
        image.unlockFocus()
        return image
    }

    let rect = CGRect(x: 0, y: 0, width: px, height: px)
    let corner = px * 0.10

    let bgPath = NSBezierPath(roundedRect: rect, xRadius: corner, yRadius: corner)
    bgPath.addClip()

    let top = NSColor(calibratedRed: 0.22, green: 0.28, blue: 0.32, alpha: 1.0)
    let bottom = NSColor(calibratedRed: 0.17, green: 0.21, blue: 0.24, alpha: 1.0)
    let gradient = NSGradient(starting: top, ending: bottom)!
    gradient.draw(in: bgPath, angle: -90)

    let panelRect = CGRect(x: px * 0.04, y: px * 0.04, width: px * 0.92, height: px * 0.92)
    ctx.setFillColor(NSColor(calibratedWhite: 1.0, alpha: 0.025).cgColor)
    ctx.fill(panelRect)

    let shadow = NSShadow()
    shadow.shadowColor = NSColor(calibratedWhite: 0, alpha: 0.22)
    shadow.shadowBlurRadius = px * 0.05
    shadow.shadowOffset = NSSize(width: 0, height: -px * 0.015)
    shadow.set()

    let orange = NSColor(calibratedRed: 0.98, green: 0.48, blue: 0.08, alpha: 1.0)
    let orangeDark = NSColor(calibratedRed: 0.86, green: 0.35, blue: 0.05, alpha: 1.0)
    let cyan = NSColor(calibratedRed: 0.18, green: 0.78, blue: 0.96, alpha: 1.0)
    let cyanDark = NSColor(calibratedRed: 0.10, green: 0.58, blue: 0.77, alpha: 1.0)

    let leftM = NSBezierPath()
    leftM.move(to: CGPoint(x: px * 0.14, y: px * 0.22))
    leftM.line(to: CGPoint(x: px * 0.14, y: px * 0.74))
    leftM.line(to: CGPoint(x: px * 0.62, y: px * 0.74))
    leftM.line(to: CGPoint(x: px * 0.62, y: px * 0.58))
    leftM.line(to: CGPoint(x: px * 0.34, y: px * 0.58))
    leftM.line(to: CGPoint(x: px * 0.34, y: px * 0.36))
    leftM.line(to: CGPoint(x: px * 0.48, y: px * 0.36))
    leftM.line(to: CGPoint(x: px * 0.48, y: px * 0.22))
    leftM.close()
    orange.setFill()
    leftM.fill()

    let leftShade = NSBezierPath()
    leftShade.move(to: CGPoint(x: px * 0.14, y: px * 0.22))
    leftShade.line(to: CGPoint(x: px * 0.22, y: px * 0.22))
    leftShade.line(to: CGPoint(x: px * 0.22, y: px * 0.74))
    leftShade.line(to: CGPoint(x: px * 0.14, y: px * 0.74))
    leftShade.close()
    orangeDark.setFill()
    leftShade.fill()

    let rightM = NSBezierPath()
    rightM.move(to: CGPoint(x: px * 0.42, y: px * 0.18))
    rightM.line(to: CGPoint(x: px * 0.42, y: px * 0.50))
    rightM.line(to: CGPoint(x: px * 0.88, y: px * 0.50))
    rightM.line(to: CGPoint(x: px * 0.88, y: px * 0.34))
    rightM.line(to: CGPoint(x: px * 0.74, y: px * 0.34))
    rightM.line(to: CGPoint(x: px * 0.74, y: px * 0.18))
    rightM.line(to: CGPoint(x: px * 0.60, y: px * 0.18))
    rightM.line(to: CGPoint(x: px * 0.60, y: px * 0.34))
    rightM.line(to: CGPoint(x: px * 0.42, y: px * 0.34))
    rightM.close()
    cyan.setFill()
    rightM.fill()

    let rightShade = NSBezierPath()
    rightShade.move(to: CGPoint(x: px * 0.42, y: px * 0.18))
    rightShade.line(to: CGPoint(x: px * 0.50, y: px * 0.18))
    rightShade.line(to: CGPoint(x: px * 0.50, y: px * 0.50))
    rightShade.line(to: CGPoint(x: px * 0.42, y: px * 0.50))
    rightShade.close()
    cyanDark.setFill()
    rightShade.fill()

    image.unlockFocus()
    return image
}

func pngData(from image: NSImage, size: Int) -> Data? {
    let target = NSSize(width: size, height: size)
    let rep = NSBitmapImageRep(
        bitmapDataPlanes: nil,
        pixelsWide: Int(target.width),
        pixelsHigh: Int(target.height),
        bitsPerSample: 8,
        samplesPerPixel: 4,
        hasAlpha: true,
        isPlanar: false,
        colorSpaceName: .deviceRGB,
        bytesPerRow: 0,
        bitsPerPixel: 0
    )
    guard let bitmap = rep else { return nil }
    bitmap.size = target

    NSGraphicsContext.saveGraphicsState()
    guard let context = NSGraphicsContext(bitmapImageRep: bitmap) else {
        NSGraphicsContext.restoreGraphicsState()
        return nil
    }
    NSGraphicsContext.current = context
    NSColor.clear.setFill()
    NSBezierPath(rect: NSRect(origin: .zero, size: target)).fill()
    image.draw(in: NSRect(origin: .zero, size: target), from: .zero, operation: .copy, fraction: 1.0)
    context.flushGraphics()
    NSGraphicsContext.restoreGraphicsState()
    return bitmap.representation(using: .png, properties: [:])
}

do {
    try fm.createDirectory(at: iconsetURL, withIntermediateDirectories: true)
    let specs: [(String, Int, CGFloat)] = [
        ("icon_16x16.png", 16, 1),
        ("icon_16x16@2x.png", 32, 2),
        ("icon_32x32.png", 32, 1),
        ("icon_32x32@2x.png", 64, 2),
        ("icon_128x128.png", 128, 1),
        ("icon_128x128@2x.png", 256, 2),
        ("icon_256x256.png", 256, 1),
        ("icon_256x256@2x.png", 512, 2),
        ("icon_512x512.png", 512, 1),
        ("icon_512x512@2x.png", 1024, 2)
    ]

    let baseImage = drawIcon(size: 1024, scale: 1.0)
    for (name, pixelSize, _) in specs {
        guard let data = pngData(from: baseImage, size: pixelSize) else {
            throw NSError(domain: "ManageMovieIcon", code: 2, userInfo: [NSLocalizedDescriptionKey: "PNG render failed"])
        }
        try data.write(to: iconsetURL.appendingPathComponent(name))
    }

    let task = Process()
    task.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
    task.arguments = ["-c", "icns", iconsetURL.path, "-o", outputPath]
    try task.run()
    task.waitUntilExit()
    if task.terminationStatus != 0 {
        throw NSError(domain: "ManageMovieIcon", code: Int(task.terminationStatus), userInfo: [NSLocalizedDescriptionKey: "iconutil failed"])
    }
} catch {
    fputs("generate_manage_movie_icon.swift failed: \(error.localizedDescription)\n", stderr)
    exit(1)
}
