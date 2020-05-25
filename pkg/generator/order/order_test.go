// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package order

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/simhospital/pkg/config"
	"github.com/google/simhospital/pkg/constants"
	"github.com/google/simhospital/pkg/doctor"
	"github.com/google/simhospital/pkg/message"
	"github.com/google/simhospital/pkg/orderprofile"
	"github.com/google/simhospital/pkg/pathway"
	"github.com/google/simhospital/pkg/test"
	"github.com/google/simhospital/pkg/test/testwrite"
)

const seqID = "1"

var (
	eventTime = time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC)

	ureaElectrolytesCE = &message.CodedElement{ID: "lpdc-3969", Text: "UREA AND ELECTROLYTES", CodingSystem: "WinPath"}
	creatinineCE       = &message.CodedElement{ID: "lpdc-2012", Text: "Creatinine", CodingSystem: "WinPath"}
	potassiumCE        = &message.CodedElement{ID: "lpdc-2804", Text: "Potassium", CodingSystem: "WinPath"}
	hydroxyCE          = &message.CodedElement{ID: "OHPROG", Text: "17-Hydroxy Progesterone", CodingSystem: "WinPath"}

	creatinineRange = "49 - 92"
	hydroxyRange    = "<=9.6^^<=9.6"

	singleDoctorData = []byte(`
- id: "id-1"
  surname: "surname-1"
  firstname: "firstname-1"
  prefix: "prefix-1"
  specialty: "specialty-1"`)
	singleDoctor = &message.Doctor{
		ID:        "id-1",
		Surname:   "surname-1",
		FirstName: "firstname-1",
		Prefix:    "prefix-1",
		Specialty: "specialty-1",
	}
)

func TestNewOrder(t *testing.T) {
	b := []byte(`
UREA AND ELECTROLYTES:
  universal_service_id: lpdc-3969
  test_types:
    Creatinine:
      id: lpdc-2012
      value_type: NM
      value: 51
      unit: UMOLL
      ref_range: 49 - 92`)
	op := testwrite.BytesToFile(t, b)

	g, hl7Config := testGeneratorWithOrderProfile(t, op)

	cases := []struct {
		name            string
		pathway         *pathway.Order
		wantOrderStatus string
		wantOP          *message.CodedElement
	}{
		{
			name:            "Existing order profile",
			pathway:         &pathway.Order{OrderProfile: "UREA AND ELECTROLYTES"},
			wantOrderStatus: hl7Config.OrderStatus.InProcess,
			wantOP:          &message.CodedElement{ID: "lpdc-3969", Text: "UREA AND ELECTROLYTES", CodingSystem: "WinPath"},
		}, {
			name:            "No matching order profile",
			pathway:         &pathway.Order{OrderProfile: "Foo"},
			wantOrderStatus: hl7Config.OrderStatus.InProcess,
			wantOP:          &message.CodedElement{ID: "Foo", Text: "Foo"},
		}, {
			name:            "Random order profile",
			pathway:         &pathway.Order{OrderProfile: constants.RandomString},
			wantOrderStatus: hl7Config.OrderStatus.InProcess,
			wantOP:          &message.CodedElement{ID: "lpdc-3969", Text: "UREA AND ELECTROLYTES", CodingSystem: "WinPath"},
		}, {
			name:            "OrderStatus explicitly specified",
			pathway:         &pathway.Order{OrderProfile: constants.RandomString, OrderStatus: "DISPATCH"},
			wantOrderStatus: "DISPATCH",
			wantOP:          &message.CodedElement{ID: "lpdc-3969", Text: "UREA AND ELECTROLYTES", CodingSystem: "WinPath"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Order{
				OrderProfile:          tc.wantOP,
				Placer:                seqID,
				OrderDateTime:         message.NewValidTime(eventTime),
				OrderControl:          hl7Config.OrderControl.New,
				OrderStatus:           tc.wantOrderStatus,
				CollectedDateTime:     message.NewInvalidTime(),
				ReceivedInLabDateTime: message.NewInvalidTime(),
				ReportedDateTime:      message.NewInvalidTime(),
			}
			got := g.NewOrder(tc.pathway, eventTime)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("g.NewOrder(%v, %v) diff: (-want, +got):\n%s", tc.pathway, eventTime, diff)
			}
		})
	}
}

func TestOrderWithClinicalNote(t *testing.T) {
	hl7Config, err := config.LoadHL7Config(test.MessageConfigTest)
	if err != nil {
		t.Fatalf("LoadHL7Config(%s) failed with %v", test.MessageConfigTest, err)
	}
	op, err := orderprofile.Load(test.OrderProfilesConfigTest, hl7Config)
	if err != nil {
		t.Fatalf("orderprofile.Load(%s, %+v) failed with %v", test.OrderProfilesConfigTest, hl7Config, err)
	}

	// Loading single doctor to eliminate randomness.
	tmpDoctors := testwrite.BytesToFile(t, singleDoctorData)
	d, err := doctor.LoadDoctors(tmpDoctors)
	if err != nil {
		t.Fatalf("LoadDoctors(%s) failed with %v", tmpDoctors, err)
	}

	wantTitle := "document-title"
	wantClinicalNoteContent := &message.ClinicalNoteContent{
		DocumentContent:     "rtf-content",
		ContentType:         "rtf",
		ObservationDateTime: message.NewValidTime(eventTime),
	}
	addendum := &message.ClinicalNoteContent{
		DocumentContent:     "pdf-content",
		ContentType:         "pdf",
		ObservationDateTime: message.NewValidTime(eventTime),
	}
	wantClinicalNote := &message.ClinicalNote{
		DocumentType:  "document-type",
		DocumentID:    "doc-id",
		DocumentTitle: wantTitle,
		DateTime:      message.NewValidTime(eventTime),
		Contents:      []*message.ClinicalNoteContent{wantClinicalNoteContent},
	}

	cn := &pathway.ClinicalNote{
		ContentType:   wantClinicalNote.Contents[0].ContentType,
		DocumentID:    wantClinicalNote.DocumentID,
		DocumentTitle: wantTitle,
	}

	cases := []struct {
		name                string
		notePathway         *pathway.ClinicalNote
		existingOrder       *message.Order
		notesGeneratorError error
		wantClinicalNote    *message.ClinicalNote
		want                *message.Order
		wantErr             bool
	}{
		{
			name:             "new clinical note success",
			notePathway:      cn,
			wantClinicalNote: wantClinicalNote,
			want: &message.Order{
				Results: []*message.Result{{
					ClinicalNote: &message.ClinicalNote{
						DateTime:      message.NewValidTime(eventTime),
						Contents:      []*message.ClinicalNoteContent{wantClinicalNoteContent},
						DocumentType:  wantClinicalNote.DocumentType,
						DocumentID:    wantClinicalNote.DocumentID,
						DocumentTitle: wantClinicalNote.DocumentTitle,
					},
				}},
				OrderProfile: &message.CodedElement{
					ID:            wantClinicalNote.DocumentType,
					Text:          wantClinicalNote.DocumentType,
					AlternateText: wantClinicalNote.DocumentTitle,
				},
				ResultsStatus:    "AUTHVRF",
				DiagnosticServID: "MDOC",
				OrderingProvider: singleDoctor,
			},
		}, {
			name: "update clinical note success",
			notePathway: &pathway.ClinicalNote{
				DocumentType:  "new-type",
				DocumentTitle: "new-title",
				DocumentID:    wantClinicalNote.DocumentID,
			},
			existingOrder: &message.Order{
				Results: []*message.Result{{
					ClinicalNote: &message.ClinicalNote{
						DateTime:     message.NewValidTime(eventTime),
						Contents:     []*message.ClinicalNoteContent{wantClinicalNoteContent},
						DocumentType: wantClinicalNote.DocumentType,
						DocumentID:   wantClinicalNote.DocumentID,
					},
				}},
				OrderProfile: &message.CodedElement{
					ID:            wantClinicalNote.DocumentType,
					Text:          wantClinicalNote.DocumentType,
					AlternateText: wantTitle,
				},
				ResultsStatus:    "AUTHVRF",
				DiagnosticServID: "MDOC",
				OrderingProvider: singleDoctor,
			},
			wantClinicalNote: &message.ClinicalNote{
				DateTime:      message.NewValidTime(eventTime),
				Contents:      []*message.ClinicalNoteContent{wantClinicalNoteContent, addendum},
				DocumentType:  "new-type",
				DocumentTitle: "new-title",
				DocumentID:    wantClinicalNote.DocumentID,
			},
			want: &message.Order{
				Results: []*message.Result{{
					ClinicalNote: &message.ClinicalNote{
						DateTime:      message.NewValidTime(eventTime),
						Contents:      []*message.ClinicalNoteContent{wantClinicalNoteContent, addendum},
						DocumentID:    wantClinicalNote.DocumentID,
						DocumentType:  "new-type",
						DocumentTitle: "new-title",
					},
				}},
				OrderProfile: &message.CodedElement{
					ID:            "new-type",
					Text:          "new-type",
					AlternateText: "new-title",
				},
				ResultsStatus:    "AUTHVRF",
				DiagnosticServID: "MDOC",
				OrderingProvider: singleDoctor,
			},
		}, {
			name:          "order with 0 results passed to method",
			existingOrder: &message.Order{},
			wantErr:       true,
		}, {
			name: "wrong order passed to the method",
			existingOrder: &message.Order{
				Results: []*message.Result{{Value: "1"}},
			},
			wantErr: true,
		}, {
			name:                "failure",
			notesGeneratorError: errors.New("cannot generate notes"),
			wantErr:             true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ng := &fakeNoteGenerator{
				wantClinicalNote: tc.wantClinicalNote,
				wantErr:          tc.notesGeneratorError,
			}

			g := &Generator{
				MessageConfig:         hl7Config,
				OrderProfiles:         op,
				NoteGenerator:         ng,
				PlacerGenerator:       &sequenceIDGenerator{},
				FillerGenerator:       &sequenceIDGenerator{},
				AbnormalFlagConvertor: NewAbnormalFlagConvertor(hl7Config),
				Doctors:               d,
			}

			got, err := g.OrderWithClinicalNote(tc.existingOrder, cn, eventTime)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("g.OrderWithClinicalNote(%v, %+v, %v) got err %v, want ?err=%t", tc.existingOrder, cn, eventTime, err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("g.OrderWithClinicalNote(%v, %+v, %v) diff: (-want, +got):\n%s", tc.existingOrder, cn, eventTime, diff)
			}
		})
	}
}

func TestSetResultsOverrideDates(t *testing.T) {
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name                      string
		order                     *message.Order
		pathwayR                  *pathway.Results
		wantCollectedDateTime     message.NullTime
		wantReceivedInLabDateTime message.NullTime
		wantObservationDateTime   message.NullTime
	}{
		{
			name: "No order provided",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantCollectedDateTime:     message.NewValidTime(eventTime),
			wantReceivedInLabDateTime: message.NewValidTime(eventTime),
			wantObservationDateTime:   message.NewValidTime(eventTime),
		}, {
			name:  "No dates override",
			order: ureaOrder(eventTime, hl7Config),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantCollectedDateTime:     message.NewValidTime(eventTime),
			wantReceivedInLabDateTime: message.NewValidTime(eventTime),
			wantObservationDateTime:   message.NewValidTime(eventTime),
		}, {
			name:  "Empty collected and received in lab dates",
			order: ureaOrder(eventTime, hl7Config),
			pathwayR: &pathway.Results{
				OrderProfile:          "UREA AND ELECTROLYTES",
				CollectedDateTime:     constants.EmptyString,
				ReceivedInLabDateTime: constants.EmptyString,
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantCollectedDateTime:     message.NewInvalidTime(),
			wantReceivedInLabDateTime: message.NewInvalidTime(),
			wantObservationDateTime:   message.NewInvalidTime(),
		}, {
			name:  "Midnight collected and received in lab dates",
			order: ureaOrder(eventTime, hl7Config),
			pathwayR: &pathway.Results{
				OrderProfile:          "UREA AND ELECTROLYTES",
				CollectedDateTime:     constants.MidnightDate,
				ReceivedInLabDateTime: constants.MidnightDate,
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantCollectedDateTime:     message.NewMidnightTime(eventTime),
			wantReceivedInLabDateTime: message.NewMidnightTime(eventTime),
			wantObservationDateTime:   message.NewValidTime(eventTime),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Order{
				OrderProfile:          ureaElectrolytesCE,
				Placer:                seqID,
				Filler:                seqID,
				OrderControl:          hl7Config.OrderControl.New,
				OrderStatus:           hl7Config.OrderStatus.Completed,
				ResultsStatus:         hl7Config.ResultStatus.Final,
				OrderDateTime:         message.NewValidTime(eventTime),
				CollectedDateTime:     tc.wantCollectedDateTime,
				ReceivedInLabDateTime: tc.wantReceivedInLabDateTime,
				ReportedDateTime:      message.NewValidTime(eventTime),
				Results: []*message.Result{
					{
						TestName:            creatinineCE,
						Value:               "52",
						Unit:                "UMOLL",
						ValueType:           "NM",
						Range:               creatinineRange,
						Status:              hl7Config.ResultStatus.Final,
						AbnormalFlag:        "",
						ObservationDateTime: tc.wantObservationDateTime,
					},
				},
			}
			got, err := g.SetResults(tc.order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", tc.order, tc.pathwayR, eventTime, err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) diff (-want, +got):\n%s", tc.order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func TestSetResultsDifferentDates(t *testing.T) {
	orderTime := time.Date(2018, 2, 12, 1, 25, 0, 0, time.UTC)
	reportTime := time.Date(2018, 2, 12, 16, 50, 0, 0, time.UTC)
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name              string
		pathwayR          *pathway.Results
		wantObsTimeOffset time.Duration
	}{
		{
			name: "No Observation DateTime Offset",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantObsTimeOffset: 0,
		}, {
			name: "Observation DateTime Offset",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName:                  "Creatinine",
						Value:                     "52",
						Unit:                      "UMOLL",
						ObservationDateTimeOffset: 24 * time.Hour,
					},
				},
			},
			wantObsTimeOffset: 24 * time.Hour,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			order := ureaOrder(orderTime, hl7Config)

			got, err := g.SetResults(order, tc.pathwayR, reportTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", order, tc.pathwayR, reportTime, err)
			}

			if diff := cmp.Diff(message.NewValidTime(orderTime), got.OrderDateTime); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) OrderDateTime diff (-want, +got):\n%s", order, tc.pathwayR, reportTime, diff)
			}
			if diff := cmp.Diff(message.NewValidTime(reportTime), got.ReportedDateTime); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) ReportedDateTime diff (-want, +got):\n%s", order, tc.pathwayR, reportTime, diff)
			}

			// CollectedDateTime <= ReceivedInLabDateTime <= ReportedDateTime
			if !got.ReceivedInLabDateTime.Valid {
				t.Errorf("SetResults(%+v, %+v, %+v) ReceivedInLabDateTime.Valid is false", order, tc.pathwayR, reportTime)
			}
			if !isDateBetween(got.ReceivedInLabDateTime.Time, got.CollectedDateTime.Time, got.ReportedDateTime.Time) {
				t.Errorf("SetResults(%+v, %+v, %+v) ReceivedInLabDateTime.Time = %v, want in range [%v, %v]",
					order, tc.pathwayR, reportTime, got.ReceivedInLabDateTime.Time, got.CollectedDateTime.Time, got.ReportedDateTime.Time)
			}

			// OrderDateTime <= CollectedDateTime <= ReceivedInLabDateTime
			if !got.CollectedDateTime.Valid {
				t.Errorf("SetResults(%+v, %+v, %+v) CollectedDateTime.Valid is false", order, tc.pathwayR, reportTime)
			}
			if !isDateBetween(got.CollectedDateTime.Time, got.OrderDateTime.Time, got.ReceivedInLabDateTime.Time) {
				t.Errorf("SetResults(%+v, %+v, %+v) CollectedDateTime.Time = %v, want in range [%v, %v]",
					order, tc.pathwayR, reportTime, got.CollectedDateTime.Time, got.OrderDateTime.Time, got.ReceivedInLabDateTime.Time)
			}

			// ObservationDateTime = CollectedDateTime + wantwantObsTimeOffset
			if diff := cmp.Diff(message.NewValidTime(got.CollectedDateTime.Add(tc.wantObsTimeOffset)), got.Results[0].ObservationDateTime); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) ObservationDateTime diff (-want, +got):\n%s", order, tc.pathwayR, reportTime, diff)
			}
		})
	}
}

func TestSetResultsOverrideStatus(t *testing.T) {
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name              string
		pathwayR          *pathway.Results
		wantOrderStatus   string
		wantResultsStatus string
		wantResultStatus  string
	}{
		{
			name: "No status override",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:   hl7Config.OrderStatus.Completed,
			wantResultsStatus: hl7Config.ResultStatus.Final,
			wantResultStatus:  hl7Config.ResultStatus.Final,
		},
		{
			name: "Override order and result status - for order and result",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				OrderStatus:  "A",
				ResultStatus: "P",
				Results: []*pathway.Result{
					{
						TestName:     "Creatinine",
						ResultStatus: "R",
						Value:        "52",
						Unit:         "UMOLL",
					},
				},
			},
			wantOrderStatus:   "A",
			wantResultsStatus: "P",
			wantResultStatus:  "R",
		}, {
			name: "Override order and result status - only at the order level",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				OrderStatus:  "A",
				ResultStatus: "P",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:   "A",
			wantResultsStatus: "P",
			wantResultStatus:  "P",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Order{
				OrderProfile:          ureaElectrolytesCE,
				Placer:                seqID,
				Filler:                seqID,
				OrderControl:          hl7Config.OrderControl.New,
				OrderStatus:           tc.wantOrderStatus,
				ResultsStatus:         tc.wantResultsStatus,
				OrderDateTime:         message.NewValidTime(eventTime),
				CollectedDateTime:     message.NewValidTime(eventTime),
				ReceivedInLabDateTime: message.NewValidTime(eventTime),
				ReportedDateTime:      message.NewValidTime(eventTime),
				Results: []*message.Result{
					{
						TestName:            creatinineCE,
						Value:               "52",
						Unit:                "UMOLL",
						ValueType:           "NM",
						Range:               creatinineRange,
						Status:              tc.wantResultStatus,
						AbnormalFlag:        "",
						ObservationDateTime: message.NewValidTime(eventTime),
					},
				},
			}
			order := ureaOrder(eventTime, hl7Config)
			got, err := g.SetResults(order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", order, tc.pathwayR, eventTime, err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) diff (-want, +got):\n%s", order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func TestSetResultsAbnormalFlag(t *testing.T) {
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name       string
		pathwayR   *pathway.Results
		wantResult *message.Result
	}{
		{
			name: "High abnormal flag from the order profile reference range",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName:     "Creatinine",
						Value:        "100",
						Unit:         "UMOLL",
						AbnormalFlag: constants.AbnormalFlagDefault,
					},
				},
			},
			wantResult: &message.Result{
				TestName:            creatinineCE,
				Value:               "100",
				Unit:                "UMOLL",
				ValueType:           "NM",
				Range:               creatinineRange,
				Status:              hl7Config.ResultStatus.Final,
				AbnormalFlag:        hl7Config.AbnormalFlags.AboveHighNormal,
				ObservationDateTime: message.NewValidTime(eventTime),
			},
		}, {
			name: "Override abnormal flag",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName:     "Creatinine",
						Value:        "100",
						Unit:         "UMOLL",
						AbnormalFlag: constants.AbnormalFlagLow,
					},
				},
			},
			wantResult: &message.Result{
				TestName:            creatinineCE,
				Value:               "100",
				Unit:                "UMOLL",
				ValueType:           "NM",
				Range:               creatinineRange,
				Status:              hl7Config.ResultStatus.Final,
				AbnormalFlag:        hl7Config.AbnormalFlags.BelowLowNormal,
				ObservationDateTime: message.NewValidTime(eventTime),
			},
		}, {
			name: "Override reference ranges; low abnormal flag",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName:       "Creatinine",
						Value:          "100",
						Unit:           "UMOLL",
						ReferenceRange: "145 - 550",
						AbnormalFlag:   constants.AbnormalFlagDefault,
					},
				},
			},
			wantResult: &message.Result{
				TestName:            creatinineCE,
				Value:               "100",
				Unit:                "UMOLL",
				ValueType:           "NM",
				Range:               "145 - 550",
				Status:              hl7Config.ResultStatus.Final,
				AbnormalFlag:        hl7Config.AbnormalFlags.BelowLowNormal,
				ObservationDateTime: message.NewValidTime(eventTime),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Order{
				OrderProfile:          ureaElectrolytesCE,
				Placer:                seqID,
				Filler:                seqID,
				OrderControl:          hl7Config.OrderControl.New,
				OrderStatus:           hl7Config.OrderStatus.Completed,
				ResultsStatus:         hl7Config.ResultStatus.Final,
				OrderDateTime:         message.NewValidTime(eventTime),
				CollectedDateTime:     message.NewValidTime(eventTime),
				ReceivedInLabDateTime: message.NewValidTime(eventTime),
				ReportedDateTime:      message.NewValidTime(eventTime),
				Results: []*message.Result{
					tc.wantResult,
				},
			}
			order := ureaOrder(eventTime, hl7Config)
			got, err := g.SetResults(order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", order, tc.pathwayR, eventTime, err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) diff (-want, +got):\n%s", order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func TestSetResultsSetValueType(t *testing.T) {
	g, hl7Config := testGeneratorWithOrderProfile(t, test.ComplexOrderProfilesConfigTest)

	cases := []struct {
		name     string
		pathwayR *pathway.Results
		order    *message.Order
		want     *message.Result
	}{{
		name: "Empty Value",
		pathwayR: &pathway.Results{
			OrderProfile: "UREA AND ELECTROLYTES",
			Results: []*pathway.Result{
				{
					TestName: "Creatinine",
					Value:    constants.EmptyString,
					Unit:     "UMOLL",
				},
			},
		},
		order: ureaOrder(eventTime, hl7Config),
		want: &message.Result{
			TestName:            creatinineCE,
			Value:               "",
			Unit:                "UMOLL",
			ValueType:           "NM",
			Range:               creatinineRange,
			Status:              hl7Config.ResultStatus.Final,
			ObservationDateTime: message.NewValidTime(eventTime),
		},
	}, {
		name: "Override type NM with TX",
		pathwayR: &pathway.Results{
			OrderProfile: "UREA AND ELECTROLYTES",
			Results: []*pathway.Result{
				{
					TestName: "Creatinine",
					Value:    "Normal value",
				},
			},
		},
		order: ureaOrder(eventTime, hl7Config),
		want: &message.Result{
			TestName:            creatinineCE,
			Value:               "Normal value",
			Unit:                "",
			ValueType:           "TX",
			Range:               creatinineRange,
			Status:              hl7Config.ResultStatus.Final,
			ObservationDateTime: message.NewValidTime(eventTime),
		},
	}, {
		name: "Override String value",
		pathwayR: &pathway.Results{
			OrderProfile: "17-OH Prog",
			Results: []*pathway.Result{
				{
					TestName: "17-Hydroxy Progesterone",
					Value:    "Normal value",
				},
			},
		},
		want: &message.Result{
			TestName:            hydroxyCE,
			Value:               "Normal value",
			Unit:                "",
			ValueType:           "TX",
			Range:               hydroxyRange,
			Status:              hl7Config.ResultStatus.Final,
			ObservationDateTime: message.NewValidTime(eventTime),
		},
	}, {
		name: "Override TX with NM",
		pathwayR: &pathway.Results{
			OrderProfile: "17-OH Prog",
			Results: []*pathway.Result{
				{
					TestName:     "17-Hydroxy Progesterone",
					Value:        "12",
					Unit:         "UMOLL",
					AbnormalFlag: "HIGH",
				},
			},
		},
		want: &message.Result{
			TestName:            hydroxyCE,
			Value:               "12",
			Unit:                "UMOLL",
			ValueType:           "NM",
			Range:               hydroxyRange,
			AbnormalFlag:        hl7Config.AbnormalFlags.AboveHighNormal,
			Status:              hl7Config.ResultStatus.Final,
			ObservationDateTime: message.NewValidTime(eventTime),
		},
	}, {
		name: "Don't override CE",
		pathwayR: &pathway.Results{
			OrderProfile: "17-OH Prog CE",
			Results: []*pathway.Result{
				{
					TestName: "17-Hydroxy Progesterone",
					Value:    "something",
					Unit:     "",
				},
			},
		},
		want: &message.Result{
			TestName:            hydroxyCE,
			Value:               "something",
			Unit:                "",
			ValueType:           "CE",
			Range:               "",
			Status:              hl7Config.ResultStatus.Final,
			ObservationDateTime: message.NewValidTime(eventTime),
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := g.SetResults(tc.order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", tc.order, tc.pathwayR, eventTime, err)
			}
			if diff := cmp.Diff(tc.want, got.Results[0]); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) got Result diff (-want, +got):\n%s", diff)
			}
		})
	}
}

type valueRange struct {
	from float64
	to   float64
}

func TestSetResultsRandomValue(t *testing.T) {
	// Note: the simple order profile used here has only one TestType (Creatinine)
	// for UREA AND ELECTROLYTES OrderProfile.
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name             string
		pathwayR         *pathway.Results
		wantValueRange   valueRange
		wantRange        string
		wantUnit         string
		wantAbnormalFlag string
	}{
		{
			name: "Random value for all test types from order profile",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
			},
			wantValueRange:   valueRange{from: 49, to: 92},
			wantRange:        creatinineRange,
			wantUnit:         "UMOLL",
			wantAbnormalFlag: "",
		}, {
			name: "Random value for random order profile",
			pathwayR: &pathway.Results{
				OrderProfile: constants.RandomString,
			},
			wantValueRange:   valueRange{from: 49, to: 92},
			wantRange:        creatinineRange,
			wantUnit:         "UMOLL",
			wantAbnormalFlag: "",
		}, {
			name: "Random normal value from order profile reference range",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    constants.NormalValue,
					},
				},
			},
			wantValueRange:   valueRange{from: 49, to: 92},
			wantRange:        creatinineRange,
			wantUnit:         "UMOLL",
			wantAbnormalFlag: "",
		}, {
			name: "Random abnormal high value",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    constants.AbnormalHigh,
					},
				},
			},
			wantValueRange:   valueRange{from: 92, to: 920},
			wantRange:        creatinineRange,
			wantUnit:         "UMOLL",
			wantAbnormalFlag: hl7Config.AbnormalFlags.AboveHighNormal,
		}, {
			name: "Random abnormal low value",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    constants.AbnormalLow,
					},
				},
			},
			wantValueRange:   valueRange{from: 0, to: 49},
			wantRange:        creatinineRange,
			wantUnit:         "UMOLL",
			wantAbnormalFlag: hl7Config.AbnormalFlags.BelowLowNormal,
		}, {
			name: "Random value from overridden reference range",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName:       "Creatinine",
						Value:          constants.NormalValue,
						Unit:           "MOLL",
						ReferenceRange: "145 - 550",
					},
				},
			},
			wantValueRange:   valueRange{from: 145, to: 550},
			wantRange:        "145 - 550",
			wantUnit:         "MOLL",
			wantAbnormalFlag: "",
		}, {
			name: "Random value from not existing order profile",
			pathwayR: &pathway.Results{
				OrderProfile: "ARBITRARY",
				Results: []*pathway.Result{
					{
						TestName:       "Test1",
						Value:          constants.NormalValue,
						Unit:           "MOLL",
						ReferenceRange: "145 - 550",
					},
				},
			},
			wantValueRange:   valueRange{from: 145, to: 550},
			wantRange:        "145 - 550",
			wantUnit:         "MOLL",
			wantAbnormalFlag: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var order *message.Order

			got, err := g.SetResults(order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", order, tc.pathwayR, eventTime, err)
			}
			if len(got.Results) != 1 {
				t.Fatalf("SetResults(%+v, %+v, %+v) got results %v, want one result", order, tc.pathwayR, eventTime, got.Results)
			}

			gotResult := got.Results[0]
			if gotResult.Unit != tc.wantUnit {
				t.Errorf("SetResults(%+v, %+v, %+v) got Unit=%v, want %v", order, tc.pathwayR, eventTime, gotResult.Unit, tc.wantUnit)
			}
			if gotResult.Range != tc.wantRange {
				t.Errorf("SetResults(%+v, %+v, %+v) got Range=%v, want %v", order, tc.pathwayR, eventTime, gotResult.Range, tc.wantRange)
			}
			if gotResult.AbnormalFlag != tc.wantAbnormalFlag {
				t.Errorf("SetResults(%+v, %+v, %+v) got AbnormalFlag=%q, want %v", order, tc.pathwayR, eventTime, gotResult.AbnormalFlag, tc.wantAbnormalFlag)
			}
			f, err := strconv.ParseFloat(gotResult.Value, 64)
			if err != nil {
				t.Fatalf("ParseFloat(%q, 64) failed with %q", gotResult.Value, err)
			}
			if f <= tc.wantValueRange.from || f >= tc.wantValueRange.to {
				t.Errorf("SetResults(%+v, %+v, %+v) got Value %f, want in range [%f, %f]", order, tc.pathwayR, eventTime, f, tc.wantValueRange.from, tc.wantValueRange.to)
			}
		})
	}
}

func TestSetResultsWithCompexOrderProfiles(t *testing.T) {
	g, hl7Config := testGeneratorWithOrderProfile(t, test.ComplexOrderProfilesConfigTest)

	cases := []struct {
		name          string
		pathwayR      *pathway.Results
		order         *message.Order
		wantTestTypes map[string]valueRange
	}{
		{
			name: "Random results for all test types for order profile",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
			},
			wantTestTypes: map[string]valueRange{
				"Creatinine": valueRange{from: 49, to: 92},
				"Potassium":  valueRange{from: 3.5, to: 5.1},
			},
		}, {
			name: "Only specified test type included",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    constants.NormalValue,
					},
				},
			},
			wantTestTypes: map[string]valueRange{
				"Creatinine": valueRange{from: 49, to: 92},
			},
		}, {
			name:  "Only specified test type for corrected results",
			order: ureaOrderWithPotassiumResultAndStatus(eventTime, hl7Config, hl7Config.OrderStatus.Completed, hl7Config.ResultStatus.Final),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Potassium",
						Value:    constants.NormalValue,
					},
				},
			},
			wantTestTypes: map[string]valueRange{
				"Potassium": valueRange{from: 3.5, to: 5.1},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := g.SetResults(tc.order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", tc.order, tc.pathwayR, eventTime, err)
			}
			if len(got.Results) != len(tc.wantTestTypes) {
				t.Fatalf("SetResults(%+v, %+v, %+v) got results len=%d, want len=%d", tc.order, tc.pathwayR, eventTime, len(got.Results), len(tc.wantTestTypes))
			}

			for _, gotResult := range got.Results {
				wantRange, ok := tc.wantTestTypes[gotResult.TestName.Text]
				if !ok {
					t.Fatalf("SetResults(%+v, %+v, %+v) got result for=%q not defined in %v",
						tc.order, tc.pathwayR, eventTime, gotResult.TestName.Text, tc.wantTestTypes)
				}
				f, err := strconv.ParseFloat(gotResult.Value, 64)
				if err != nil {
					t.Fatalf("ParseFloat(%q, 64) failed with %q", gotResult.Value, err)
				}
				if f <= wantRange.from || f >= wantRange.to {
					t.Errorf("SetResults(%+v, %+v, %+v) got Value %f, want in range [%f, %f]", tc.order, tc.pathwayR, eventTime, f, wantRange.from, wantRange.to)
				}
			}
		})
	}
}

func TestSetResultsUnknownTestTypeOrOrderProfile(t *testing.T) {
	g, hl7Config := testGenerator(t)

	tests := []struct {
		name     string
		order    *message.Order
		pathwayR *pathway.Results
		want     []*message.Result
		wantErr  bool
	}{
		{
			name: "No matching Order Profile",
			pathwayR: &pathway.Results{
				OrderProfile: "ARBITRARY UNKNOWN ORDER PROFILE",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    "200",
						Unit:     "UML",
					},
				},
			},
			want: []*message.Result{{
				TestName:            &message.CodedElement{ID: "Bar", Text: "Bar"},
				Value:               "200",
				Unit:                "UML",
				ValueType:           "NM",
				Status:              hl7Config.ResultStatus.Final,
				ObservationDateTime: message.NewValidTime(eventTime),
			}},
		}, {
			name: "No matching Order Profile String Value",
			pathwayR: &pathway.Results{
				OrderProfile: "ARBITRARY UNKNOWN ORDER PROFILE",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    "Normal value",
					},
				},
			},
			want: []*message.Result{{
				TestName:            &message.CodedElement{ID: "Bar", Text: "Bar"},
				Value:               "Normal value",
				Unit:                "",
				ValueType:           "TX",
				Status:              hl7Config.ResultStatus.Final,
				ObservationDateTime: message.NewValidTime(eventTime),
			}},
		}, {
			name: "No matching Order Profile Empty Value",
			pathwayR: &pathway.Results{
				OrderProfile: "ARBITRARY UNKNOWN ORDER PROFILE",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    constants.EmptyString,
					},
				},
			},
			want: []*message.Result{{
				TestName:            &message.CodedElement{ID: "Bar", Text: "Bar"},
				Value:               "",
				Unit:                "",
				ValueType:           "",
				Status:              hl7Config.ResultStatus.Final,
				ObservationDateTime: message.NewValidTime(eventTime),
			}},
		}, {
			name: "No matching Order Profile for Corrected results",
			order: &message.Order{
				OrderProfile:      &message.CodedElement{ID: "lpdc-1234", Text: "Foo", CodingSystem: "WinPath"},
				Placer:            "12345",
				OrderDateTime:     message.NewValidTime(eventTime),
				CollectedDateTime: message.NewValidTime(eventTime),
				OrderStatus:       g.MessageConfig.OrderStatus.InProcess,
				ResultsStatus:     g.MessageConfig.ResultStatus.Final,
				Results: []*message.Result{
					{
						TestName:            potassiumCE,
						Value:               "3.6",
						ObservationDateTime: message.NewValidTime(eventTime),
					},
				},
			},
			pathwayR: &pathway.Results{
				OrderProfile: "Foo",
				Results: []*pathway.Result{
					{
						TestName:       "Bar",
						Value:          "200",
						Unit:           "UML",
						ReferenceRange: "120 - 250",
					},
				},
			},
			want: []*message.Result{{
				TestName:            &message.CodedElement{ID: "Bar", Text: "Bar"},
				Value:               "200",
				Unit:                "UML",
				Range:               "120 - 250",
				ValueType:           "NM",
				Status:              hl7Config.ResultStatus.Corrected,
				ObservationDateTime: message.NewValidTime(eventTime),
			}},
		}, {
			name: "Matching Order Profile but no matching Test Name",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    constants.NormalValue,
					},
				},
			},
			wantErr: true,
		}, {
			name: "Matching Order Profile but no matching Test Name, with value set",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    "200",
						Unit:     "UML",
					},
				},
			},
			wantErr: true,
		}, {
			name:  "No matching test name for corrected order",
			order: ureaOrderWithPotassiumResult(eventTime, hl7Config),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Bar",
						Value:    "200",
						Unit:     "UML",
					},
				},
			},
			wantErr: true,
		}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, err := g.SetResults(tc.order, tc.pathwayR, eventTime)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("SetResults(%v, %v, %v) got error %v, want error? %t", tc.order, tc.pathwayR, eventTime, err, tc.wantErr)
			}
			if gotErr || tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.want, o.Results); diff != "" {
				t.Errorf("SetResults(%v, %v, %v) got Results diff (-want, +got):\n%s", tc.order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func TestSetResultsCorrectedResults(t *testing.T) {
	preliminaryResultStatus := "P"
	// availableOrderStatus means that some, but not all, results are available.
	availableOrderStatus := "A"
	before := eventTime.Add(-24 * time.Hour)
	g, hl7Config := testGenerator(t)

	cases := []struct {
		name             string
		order            *message.Order
		pathwayR         *pathway.Results
		wantOrderStatus  string
		wantResultStatus string
	}{
		{
			name:  "Correct final results - status derived as Corrected",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.Completed, hl7Config.ResultStatus.Final),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  hl7Config.OrderStatus.Completed,
			wantResultStatus: hl7Config.ResultStatus.Corrected,
		}, {
			name:  "Correct final results - status set explicitly",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.Completed, hl7Config.ResultStatus.Final),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				OrderStatus:  "A",
				ResultStatus: preliminaryResultStatus,
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  availableOrderStatus,
			wantResultStatus: preliminaryResultStatus,
		}, {
			name:  "Correct corrected results - status derived as Corrected",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.Completed, hl7Config.ResultStatus.Corrected),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  hl7Config.OrderStatus.Completed,
			wantResultStatus: hl7Config.ResultStatus.Corrected,
		}, {
			name:  "Correct corrected results - status set explicitly",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.Completed, hl7Config.ResultStatus.Corrected),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				OrderStatus:  availableOrderStatus,
				ResultStatus: preliminaryResultStatus,
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  availableOrderStatus,
			wantResultStatus: preliminaryResultStatus,
		}, {
			name:  "Correct preliminary results - status derived as Final",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.InProcess, preliminaryResultStatus),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  hl7Config.OrderStatus.Completed,
			wantResultStatus: hl7Config.ResultStatus.Final,
		}, {
			name:  "Correct preliminary results - status set explicitly",
			order: ureaOrderWithPotassiumResultAndStatus(before, hl7Config, hl7Config.OrderStatus.InProcess, preliminaryResultStatus),
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				OrderStatus:  availableOrderStatus,
				ResultStatus: preliminaryResultStatus,
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantOrderStatus:  availableOrderStatus,
			wantResultStatus: preliminaryResultStatus,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Order{
				OrderProfile:          ureaElectrolytesCE,
				Placer:                seqID,
				Filler:                seqID,
				OrderDateTime:         message.NewValidTime(before),
				CollectedDateTime:     message.NewValidTime(before),
				ReceivedInLabDateTime: message.NewValidTime(before),
				ReportedDateTime:      message.NewValidTime(eventTime),
				OrderStatus:           tc.wantOrderStatus,
				ResultsStatus:         tc.wantResultStatus,
				Results: []*message.Result{
					{
						TestName:            creatinineCE,
						Value:               "52",
						Unit:                "UMOLL",
						ValueType:           "NM",
						Range:               "49 - 92",
						ObservationDateTime: message.NewValidTime(before),
						Status:              tc.wantResultStatus,
					},
				},
			}

			got, err := g.SetResults(tc.order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%v, %v, %v) failed with %v", tc.order, tc.pathwayR, eventTime, err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) diff (-want, +got):\n%s", tc.order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func TestSetResultsOverrideNotes(t *testing.T) {
	defaultNotes := []string{"note-1", "note-2"}
	pathwayNotes := []string{"note", "from", "pathway"}

	g, hl7Config := testGenerator(t)
	g.NoteGenerator = &fakeNoteGenerator{
		wantNotes: defaultNotes,
	}

	cases := []struct {
		name         string
		pathwayR     *pathway.Results
		wantTestName *message.CodedElement
		wantNotes    []string
	}{
		{
			name: "No notes provided",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
					},
				},
			},
			wantTestName: creatinineCE,
			wantNotes:    defaultNotes,
		}, {
			name: "Notes provided in the pathway",
			pathwayR: &pathway.Results{
				OrderProfile: "UREA AND ELECTROLYTES",
				Results: []*pathway.Result{
					{
						TestName: "Creatinine",
						Value:    "52",
						Unit:     "UMOLL",
						Notes:    pathwayNotes,
					},
				},
			},
			wantTestName: creatinineCE,
			wantNotes:    pathwayNotes,
		}, {
			name: "Notes provided in the pathway for non-existing order profile",
			pathwayR: &pathway.Results{
				OrderProfile: "ARBITRARY",
				Results: []*pathway.Result{
					{
						ID:             "lpdc-0001",
						TestName:       "Test1",
						Value:          "52",
						Unit:           "UMOLL",
						ReferenceRange: creatinineRange,
						Notes:          pathwayNotes,
					},
				},
			},
			wantTestName: &message.CodedElement{ID: "lpdc-0001", Text: "Test1"},
			wantNotes:    pathwayNotes,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &message.Result{
				TestName:            tc.wantTestName,
				Value:               "52",
				Unit:                "UMOLL",
				ValueType:           "NM",
				Range:               creatinineRange,
				Status:              hl7Config.ResultStatus.Final,
				ObservationDateTime: message.NewValidTime(eventTime),
				Notes:               tc.wantNotes,
			}
			var order *message.Order
			got, err := g.SetResults(order, tc.pathwayR, eventTime)
			if err != nil {
				t.Fatalf("SetResults(%+v, %+v, %+v) failed with %v", order, tc.pathwayR, eventTime, err)
			}
			if len(got.Results) != 1 {
				t.Fatalf("SetResults(%+v, %+v, %+v) got results %v, want one result", order, tc.pathwayR, eventTime, got.Results)
			}
			if diff := cmp.Diff(want, got.Results[0]); diff != "" {
				t.Errorf("SetResults(%+v, %+v, %+v) diff (-want, +got):\n%s", order, tc.pathwayR, eventTime, diff)
			}
		})
	}
}

func testGenerator(t *testing.T) (*Generator, *config.HL7Config) {
	t.Helper()
	return testGeneratorWithOrderProfile(t, test.OrderProfilesConfigTest)
}

func testGeneratorWithOrderProfile(t *testing.T, orderProfileConfig string) (*Generator, *config.HL7Config) {
	t.Helper()
	hl7Config, err := config.LoadHL7Config(test.MessageConfigTest)
	if err != nil {
		t.Fatalf("LoadHL7Config(%s) failed with %v", test.MessageConfigTest, err)
	}
	op, err := orderprofile.Load(orderProfileConfig, hl7Config)
	if err != nil {
		t.Fatalf("orderprofile.Load(%s, %+v) failed with %v", orderProfileConfig, hl7Config, err)
	}
	d, err := doctor.LoadDoctors(test.DoctorsConfigTest)
	if err != nil {
		t.Fatalf("LoadDoctors(%s) failed with %v", test.DoctorsConfigTest, err)
	}
	ng := &fakeNoteGenerator{}

	g := &Generator{
		MessageConfig:         hl7Config,
		OrderProfiles:         op,
		NoteGenerator:         ng,
		PlacerGenerator:       &sequenceIDGenerator{},
		FillerGenerator:       &sequenceIDGenerator{},
		AbnormalFlagConvertor: NewAbnormalFlagConvertor(hl7Config),
		Doctors:               d,
	}
	return g, hl7Config
}

func ureaOrder(eventTime time.Time, c *config.HL7Config) *message.Order {
	return &message.Order{
		OrderProfile:  ureaElectrolytesCE,
		Placer:        seqID,
		OrderDateTime: message.NewValidTime(eventTime),
		OrderControl:  c.OrderControl.New,
		OrderStatus:   c.OrderStatus.InProcess,
	}
}

func ureaOrderWithPotassiumResult(t time.Time, c *config.HL7Config) *message.Order {
	return &message.Order{
		OrderProfile:          ureaElectrolytesCE,
		Placer:                seqID,
		OrderDateTime:         message.NewValidTime(t),
		CollectedDateTime:     message.NewValidTime(t),
		ReceivedInLabDateTime: message.NewValidTime(t),
		ReportedDateTime:      message.NewValidTime(t),
		OrderStatus:           c.OrderStatus.InProcess,
		ResultsStatus:         c.ResultStatus.Final,
		Results: []*message.Result{
			{
				TestName: potassiumCE,
				Value:    "3.6",
			},
		},
	}
}

func ureaOrderWithPotassiumResultAndStatus(t time.Time, c *config.HL7Config, os string, rs string) *message.Order {
	o := ureaOrderWithPotassiumResult(t, c)
	o.OrderStatus = os
	o.ResultsStatus = rs
	return o
}

type sequenceIDGenerator struct{}

func (g *sequenceIDGenerator) NewID() string {
	return seqID
}

type fakeNoteGenerator struct {
	wantNotes        []string
	wantClinicalNote *message.ClinicalNote
	wantErr          error
}

func (ng *fakeNoteGenerator) RandomNotesForResult() []string {
	return ng.wantNotes
}

func (ng *fakeNoteGenerator) RandomDocumentForClinicalNote(*pathway.ClinicalNote, *message.ClinicalNote, time.Time) (*message.ClinicalNote, error) {
	return ng.wantClinicalNote, ng.wantErr
}

// isDateBetween returns whether actual is in the range [earliest, latest]
func isDateBetween(actual time.Time, earliest time.Time, latest time.Time) bool {
	return (actual == earliest || actual.After(earliest)) && (latest.After(actual) || actual == latest)
}
