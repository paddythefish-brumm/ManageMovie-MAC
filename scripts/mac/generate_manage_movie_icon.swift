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

    let top = NSColor(calibratedRed: 0.05, green: 0.30, blue: 0.42, alpha: 1.0)
    let bottom = NSColor(calibratedRed: 0.08, green: 0.45, blue: 0.62, alpha: 1.0)
    let gradient = NSGradient(starting: top, ending: bottom)!
    gradient.draw(in: bgPath, angle: -90)

    ctx.setFillColor(NSColor(calibratedWhite: 1.0, alpha: 0.04).cgColor)
    let stripeWidth = px / 4.0
    for i in 0..<4 {
        let stripe = CGRect(x: CGFloat(i) * stripeWidth, y: px * 0.26, width: stripeWidth * 0.92, height: px * 0.74)
        ctx.fill(stripe)
    }

    let bottomBarHeight = px * 0.25
    let barRect = CGRect(x: 0, y: 0, width: px, height: bottomBarHeight)
    ctx.setFillColor(NSColor(calibratedRed: 0.97, green: 0.84, blue: 0.22, alpha: 1.0).cgColor)
    ctx.fill(barRect)

    let dividerRect = CGRect(x: 0, y: bottomBarHeight, width: px, height: max(3, px * 0.015))
    ctx.setFillColor(NSColor(calibratedRed: 0.79, green: 0.64, blue: 0.05, alpha: 1.0).cgColor)
    ctx.fill(dividerRect)

    let strokeWidth = max(6, px * 0.03)
    let screenRect = CGRect(x: px * 0.19, y: px * 0.40, width: px * 0.62, height: px * 0.38)
    let screen = NSBezierPath(roundedRect: screenRect, xRadius: px * 0.02, yRadius: px * 0.02)
    screen.lineWidth = strokeWidth
    NSColor.white.setStroke()
    screen.stroke()

    let standPath = NSBezierPath()
    standPath.lineWidth = strokeWidth
    standPath.move(to: CGPoint(x: px * 0.50, y: px * 0.39))
    standPath.line(to: CGPoint(x: px * 0.50, y: px * 0.29))
    standPath.move(to: CGPoint(x: px * 0.38, y: px * 0.23))
    standPath.line(to: CGPoint(x: px * 0.62, y: px * 0.23))
    standPath.move(to: CGPoint(x: px * 0.42, y: px * 0.29))
    standPath.line(to: CGPoint(x: px * 0.58, y: px * 0.29))
    NSColor.white.setStroke()
    standPath.stroke()

    let glare1 = NSBezierPath()
    glare1.lineWidth = max(4, strokeWidth * 0.55)
    glare1.move(to: CGPoint(x: px * 0.27, y: px * 0.68))
    glare1.line(to: CGPoint(x: px * 0.35, y: px * 0.76))
    glare1.stroke()

    let glare2 = NSBezierPath()
    glare2.lineWidth = max(3, strokeWidth * 0.4)
    glare2.move(to: CGPoint(x: px * 0.20, y: px * 0.60))
    glare2.line(to: CGPoint(x: px * 0.27, y: px * 0.67))
    glare2.stroke()

    let paragraph = NSMutableParagraphStyle()
    paragraph.alignment = .center
    let fontSize = px * 0.105
    let font = NSFont.systemFont(ofSize: fontSize, weight: .semibold)
    let attrs: [NSAttributedString.Key: Any] = [
        .font: font,
        .foregroundColor: NSColor(calibratedRed: 0.12, green: 0.18, blue: 0.26, alpha: 1.0),
        .paragraphStyle: paragraph,
        .kern: fontSize * 0.12
    ]
    let text = NSAttributedString(string: "MANAGER", attributes: attrs)
    let textRect = CGRect(x: px * 0.04, y: px * 0.055, width: px * 0.92, height: bottomBarHeight * 0.65)
    text.draw(in: textRect)

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
