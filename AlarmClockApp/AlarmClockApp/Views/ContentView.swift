import SwiftUI

struct ContentView: View {
    @StateObject var alarmViewModel = AlarmViewModel()

    var body: some View {
        AlarmListView(viewModel: alarmViewModel)
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}