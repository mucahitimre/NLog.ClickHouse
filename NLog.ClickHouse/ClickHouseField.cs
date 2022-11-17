using NLog.Config;
using NLog.Layouts;
using System.ComponentModel;

namespace NLog.ClickHouse
{
    /// <summary>
    /// A configuration item for ClickHouse target.
    /// </summary>
    [NLogConfigurationItem]
    [ThreadAgnostic]
    public sealed class ClickHouseField
    {
        private string _chType;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseField"/> class.
        /// </summary>
        public ClickHouseField()
            : this(null, null, "String")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="layout">The layout.</param>
        public ClickHouseField(string name, Layout layout)
            : this(name, layout, "String")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClickHouseField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="layout">The layout.</param>
        /// <param name="chType">Type of the ch.</param>
        public ClickHouseField(string name, Layout layout, string chType)
        {
            Name = name;
            Layout = layout;
            CHColumnType = string.IsNullOrEmpty(chType) ? chType : "String";
        }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        [RequiredParameter]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the layout.
        /// </summary>
        /// <value>
        /// The layout.
        /// </value>
        [RequiredParameter]
        public Layout Layout { get; set; }

        /// <summary>
        /// Gets or sets the type of the ch column.
        /// </summary>
        /// <value>
        /// The type of the ch column.
        /// </value>
        [DefaultValue("String")]
        public string CHColumnType
        {
            get => _chType;
            set
            {
                _chType = value;
            }
        }
    }
}