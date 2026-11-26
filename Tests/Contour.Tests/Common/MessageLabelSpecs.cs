using Contour.Helpers;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
/// The message label specs.
/// </summary>
// ReSharper disable InconsistentNaming
public class MessageLabelSpecs
{
    /// <summary>
    /// The when_comparing_message_labels.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_comparing_message_labels
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_use_structural_equality.
        /// </summary>
        [Test]
        public void should_use_structural_equality()
        {
            "label".ToMessageLabel().
                Should().
                Be(MessageLabel.From("label"));
            "LABEL".ToMessageLabel().
                Should().
                Be(MessageLabel.From("label"));
            "label".ToMessageLabel().
                Should().
                NotBe(MessageLabel.From("labell"));
            "label".ToMessageLabel().
                Should().
                NotBe(MessageLabel.From("labe"));
        }

        #endregion
    }

    /// <summary>
    /// The when_comparing_message_labels_with_any_label.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_comparing_message_labels_with_any_label
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_not_be_equal.
        /// </summary>
        [Test]
        public void should_not_be_equal()
        {
            "label".ToMessageLabel().
                Should().
                NotBe(MessageLabel.Any);
            MessageLabel.Any.Should().
                NotBe("label".ToMessageLabel());
            MessageLabel.Any.Should().
                Be(MessageLabel.Any);
        }

        #endregion
    }

    /// <summary>
    /// The when_creating_message_label_from_empty_or_null_string.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_creating_message_label_from_empty_or_null_string
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_produce_empty_label.
        /// </summary>
        [Test]
        public void should_produce_empty_label()
        {
            MessageLabel label = MessageLabel.From(string.Empty);
            label.IsEmpty.Should().
                BeTrue();

            label = MessageLabel.From(null);
            label.IsEmpty.Should().
                BeTrue();
        }

        #endregion
    }

    /// <summary>
    /// The when_creating_message_label_from_string_using_different_cases.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_creating_message_label_from_string_using_different_cases
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_build_label_with_lower_case.
        /// </summary>
        [Test]
        public void should_build_label_with_lower_case()
        {
            "Label".ToMessageLabel().
                Name.Should().
                Be("label");
            "LABEL".ToMessageLabel().
                Name.Should().
                Be("label");
        }

        #endregion
    }

    /// <summary>
    /// The when_validating_alias.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_validating_alias
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_only_pass_valid_label.
        /// </summary>
        [TestCase("label", false)]
        [TestCase("13-label", false)]
        [TestCase("*", false)]
        [TestCase("***", false)]
        [TestCase(":Label*13", false)]
        [TestCase(":Label_13", true)]
        [TestCase(":Label 13", false)]
        [TestCase(":Label\n13", false)]
        [TestCase(":label", true)]
        [TestCase("_label", false)]
        [TestCase(":13_label", false)]
        [TestCase(":метка", false)]
        public void should_only_pass_valid_label(string alias, bool expectedResult)
        {
            MessageLabel.IsValidAlias(alias).Should().Be(expectedResult);
        }

        #endregion
    }

    /// <summary>
    /// The when_validating_message_label.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class when_validating_message_label
    {
        #region Public Methods and Operators

        /// <summary>
        /// The should_only_pass_valid_label.
        /// </summary>
        /// <param name="label">
        /// The label.
        /// </param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        [Test]
        [TestCase("", true)]
        [TestCase("label", true)]
        [TestCase("Label", true)]
        [TestCase("LABEL", true)]
        [TestCase("LABEL-13", true)]
        [TestCase("*", true)]
        [TestCase("***", false)]
        [TestCase("Label*13", false)]
        [TestCase("Label_13", true)]
        [TestCase("Label 13", false)]
        [TestCase("Label\n13", false)]
        [TestCase(":label", false)]
        [TestCase("13-label", false)]
        [TestCase("_label", true)]
        [TestCase("метка", false)]
        public void should_only_pass_valid_label(string label, bool expectedResult)
        {
            MessageLabel.IsValidLabel(label).Should().Be(expectedResult);
        }

        #endregion
    }
}

// ReSharper restore InconsistentNaming