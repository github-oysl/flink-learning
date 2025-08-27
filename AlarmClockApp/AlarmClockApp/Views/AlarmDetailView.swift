import SwiftUI

struct AlarmDetailView: View {
    @ObservedObject var viewModel: AlarmViewModel
    @State var alarm: Alarm = Alarm(date: Date(), label: "Alarm", isEnabled: true)
    @Environment(\.presentationMode) var presentationMode

    var body: some View {
        Form {
            DatePicker("Time", selection: $alarm.date, displayedComponents: .hourAndMinute)
            TextField("Label", text: $alarm.label)
            Toggle("Enabled", isOn: $alarm.isEnabled)
        }
        .navigationTitle("Alarm Details")
        .navigationBarItems(trailing: Button("Save") {
            viewModel.addOrUpdateAlarm(alarm)
            presentationMode.wrappedValue.dismiss()
        })
    }
}