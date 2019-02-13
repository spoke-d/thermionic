package clui_test

//go:generate mockgen -package=mocks -destination=./mocks/autocomplete.go github.com/spoke-d/thermionic/internal/clui AutoCompleteInstaller
//go:generate mockgen -package=mocks -destination=./mocks/ui.go github.com/spoke-d/thermionic/internal/clui UI
//go:generate mockgen -package=mocks -destination=./mocks/command.go github.com/spoke-d/thermionic/internal/clui Command
