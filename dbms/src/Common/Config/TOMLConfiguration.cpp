#include "TOMLConfiguration.h"

namespace DB
{

using TOMLBasePtr = std::shared_ptr<cpptoml::base>;

TOMLConfiguration::TOMLConfiguration(TOMLTablePtr toml_doc): root(toml_doc)
{
    poco_check_ptr (toml_doc);
}

bool TOMLConfiguration::getRaw(const std::string& key, std::string& value) const
{
    cpptoml::option<std::string> opt_value = root->get_qualified_as<std::string>(key);
    if (opt_value)
    {
        value = *opt_value;
        return true;
    }

    return false;
}

void TOMLConfiguration::setRaw(const std::string& key, const std::string& value)
{
    try
    {
        TOMLBasePtr elem = root->get_qualified(key);
        if (!elem->is_value())
        {
            throw NotFoundException("Key does not point to a value", key);
        }
        elem->
    }
    catch (std::out_of_range e)
    {
        throw NotFoundException("Node not found in TOMLConfiguration", key);
    }



    std::string::const_iterator it = key.begin();
    Poco::XML::Node* pNode = findNode(it, key.end(), _pRoot, true);
    if (pNode)
    {
        unsigned short nodeType = pNode->nodeType();
        if (Poco::XML::Node::ATTRIBUTE_NODE == nodeType)
        {
            pNode->setNodeValue(value);
        }
        else if (Poco::XML::Node::ELEMENT_NODE == nodeType)
        {
            Poco::XML::Node* pChildNode = pNode->firstChild();
            if (pChildNode)
            {
                if (Poco::XML::Node::TEXT_NODE == pChildNode->nodeType())
                {
                    pChildNode->setNodeValue(value);
                }
            }
            else
            {
                Poco::AutoPtr<Poco::XML::Node> pText = _pDocument->createTextNode(value);
                pNode->appendChild(pText);
            }
        }
    }
    else throw NotFoundException("Node not found in XMLConfiguration", key);
}

void TOMLConfiguration::enumerate(const std::string& key, Keys& range) const
{
    using Poco::NumberFormatter;

    std::multiset<std::string> keySet;
    const Poco::XML::Node* pNode = findNode(key);
    if (pNode)
    {
        const Poco::XML::Node* pChild = pNode->firstChild();
        while (pChild)
        {
            if (pChild->nodeType() == Poco::XML::Node::ELEMENT_NODE)
            {
                const std::string& nodeName = pChild->nodeName();
                int n = (int) keySet.count(nodeName);
                if (n)
                    range.push_back(nodeName + "[" + NumberFormatter::format(n) + "]");
                else
                    range.push_back(nodeName);
                keySet.insert(nodeName);
            }
            pChild = pChild->nextSibling();
        }
    }
}

void TOMLConfiguration::removeRaw(const std::string& key)
{
    Poco::XML::Node* pNode = findNode(key);

    if (pNode)
    {
        if (pNode->nodeType() == Poco::XML::Node::ELEMENT_NODE)
        {
            Poco::XML::Node* pParent = pNode->parentNode();
            if (pParent)
            {
                pParent->removeChild(pNode);
            }
        }
        else if (pNode->nodeType() == Poco::XML::Node::ATTRIBUTE_NODE)
        {
            Poco::XML::Attr* pAttr = dynamic_cast<Poco::XML::Attr*>(pNode);
            Poco::XML::Element* pOwner = pAttr->ownerElement();
            if (pOwner)
            {
                pOwner->removeAttributeNode(pAttr);
            }
        }
    }
}

}