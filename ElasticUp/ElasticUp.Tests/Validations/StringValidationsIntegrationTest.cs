using ElasticUp.Util;
using ElasticUp.Validations;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Validations
{
    [TestFixture]
    public class StringValidationsIntegrationTest : AbstractIntegrationTest
    {
        private StringValidations _validations;

        [SetUp]
        public void Setup()
        {
            _validations = StringValidations.StringValidationsFor<StringValidationsIntegrationTest>();
        }

        [Test]
        public void GivenStringValue_WhenValidatingIsNotBlank_ThrowsExceptionWhenNullOrEmptyOrWhitespace()
        {
            Assert.Throws<ElasticUpException>(() => _validations.IsNotBlank(null));
            Assert.Throws<ElasticUpException>(() => _validations.IsNotBlank(""));
            Assert.Throws<ElasticUpException>(() => _validations.IsNotBlank("  "));
            Assert.DoesNotThrow(() => _validations.IsNotBlank("valid-index-name"));
        }

        [Test]
        public void GivenStringValue_WhenValidatingIsNotBlank_ThrowsExceptionWhenNullOrEmptyOrWhitespace_WithGivenMessagePrefixedByClassNameOfOperation()
        {
            try
            {
                _validations.IsNotBlank(null, "message");
                Assert.Fail("should have thrown exception");
            }
            catch (ElasticUpException exception)
            {
                exception.Message.Should().Be("StringValidationsIntegrationTest: message");
            }
        }

        [Test]
        public void GivenStringValue_WhenValidatingIsBlank_ThrowsExceptionWhenNullOrEmptyOrWhitespace()
        {
            Assert.Throws<ElasticUpException>(() => _validations.IsBlank("b"));
            Assert.DoesNotThrow(() => _validations.IsBlank(null));
            Assert.DoesNotThrow(() => _validations.IsBlank("   "));
            Assert.DoesNotThrow(() => _validations.IsBlank(""));
        }
    }
}