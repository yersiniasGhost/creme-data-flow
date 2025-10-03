class TimeFrame
  include Mongoid::Document

  field :start, type: String
  field :end, type: String

  embedded_in :rate
end

class Rate
  include Mongoid::Document

  field :name, type: String
  field :rate, type: BSON::ObjectId

  embeds_many :time_frames

  embedded_in :utility_rate_structure
end

class UtilityRateStructure
  include Mongoid::Document
  include Mongoid::Timestamps

  field :_type, type: String, default: "UtilityRateStructure"
  field :utility_id, type: BSON::ObjectId
  field :utility_data_source, type: BSON::ObjectId
  field :name, type: String
  field :seasons, type: Hash, default: {}

  # Optional fields
  field :weekdays, type: Array, default: []
  field :holidays_weekends, type: Array, default: []

  embeds_many :rate_structure, class_name: 'Rate'

  validates :name, presence: true
  validates :utility_id, presence: true
  validates :utility_data_source, presence: true
end
