import Foundation

struct Alarm: Identifiable {
    let id = UUID()
    var date: Date
    var label: String
    var isEnabled: Bool
}