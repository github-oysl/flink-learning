import SwiftUI

struct AlarmListView: View {
    @ObservedObject var viewModel: AlarmViewModel

    var body: some View {
        NavigationView {
            List {
                ForEach(viewModel.alarms) { alarm in
                    NavigationLink(destination: AlarmDetailView(viewModel: viewModel, alarm: alarm)) {
                        HStack {
                            VStack(alignment: .leading) {
                                Text(alarm.date, style: .time)
                                    .font(.largeTitle)
                                Text(alarm.label)
                            }
                            Spacer()
                            Toggle("", isOn: .constant(alarm.isEnabled))
                        }
                    }
                }
                .onDelete(perform: viewModel.deleteAlarm)
            }
            .navigationTitle("Alarms")
            .navigationBarItems(trailing: 
                NavigationLink(destination: AlarmDetailView(viewModel: viewModel)) {
                    Image(systemName: "plus")
                }
            )
        }
    }
}