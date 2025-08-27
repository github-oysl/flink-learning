import Foundation

class AlarmViewModel: ObservableObject {
    @Published var alarms: [Alarm] = []

    func addOrUpdateAlarm(_ alarm: Alarm) {
        if let index = alarms.firstIndex(where: { $0.id == alarm.id }) {
            alarms[index] = alarm
        } else {
            alarms.append(alarm)
        }
    }

    func deleteAlarm(at offsets: IndexSet) {
        alarms.remove(atOffsets: offsets)
    }
}